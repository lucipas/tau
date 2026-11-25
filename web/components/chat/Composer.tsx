// Composer.tsx — chat composer: text, send-later scheduling, disappearing
// expiry picker, file attach (≤10 MiB, enforced client-side), voice-note
// recording (MediaRecorder, sent as a voice-marked file), poll mode,
// location-pin mode (latitude/longitude + optional place name), and
// contact mode (share a known peer's contact card).

"use client";

import { useEffect, useRef, useState } from "react";
import { formatBytes } from "@/lib/format";
import type { ChatMessage } from "@/lib/types";

/** Bridge file cap: POST /api/send-file rejects files over 10 MiB. */
export const MAX_FILE_BYTES = 10 * 1024 * 1024;

export function validateFile(file: File): string | null {
  if (file.size > MAX_FILE_BYTES) {
    return `File is ${formatBytes(file.size)} — the limit is ${formatBytes(MAX_FILE_BYTES)}.`;
  }
  if (file.size === 0) return "That file is empty.";
  return null;
}

/** Elapsed recording time as m:ss, for the stop button. */
export function formatVoiceDuration(seconds: number): string {
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return `${minutes}:${String(rest).padStart(2, "0")}`;
}

/** True when this browser can record a voice note. */
export function canRecordVoice(): boolean {
  return (
    typeof window !== "undefined" &&
    typeof MediaRecorder !== "undefined" &&
    typeof navigator !== "undefined" &&
    !!navigator.mediaDevices?.getUserMedia
  );
}

/** A live voice recording: its recorder, its microphone stream, and a
    flag marking it abandoned (unmount, chat change, runtime error) so
    its output is never attached. */
type VoiceSession = {
  recorder: MediaRecorder;
  stream: MediaStream;
  discard: boolean;
};

/** Abandon a voice recording: stop the recorder (its onstop sees the
    discard flag and drops the output) and release the microphone. */
function releaseVoiceSession(session: VoiceSession): void {
  session.discard = true;
  if (session.recorder.state !== "inactive") {
    try {
      session.recorder.stop();
    } catch {
      // Already stopped or errored; the tracks still need releasing.
    }
  }
  session.stream.getTracks().forEach((t) => t.stop());
}

export function validatePoll(question: string, options: string[]): string | null {
  if (!question.trim()) return "A poll needs a question.";
  const filled = options.map((o) => o.trim()).filter(Boolean);
  if (filled.length < 2) return "A poll needs at least two options.";
  if (filled.length > 10) return "A poll can have at most ten options.";
  return null;
}

/** A location pin is decimal degrees; the place name is at most 200 characters. */
export function validateLocation(lat: string, lon: string, name: string): string | null {
  if (lat.trim() === "") return "Latitude is required.";
  const latitude = Number(lat);
  if (!Number.isFinite(latitude)) return "Latitude must be a number.";
  if (latitude < -90 || latitude > 90) return "Latitude must be between -90 and 90.";
  if (lon.trim() === "") return "Longitude is required.";
  const longitude = Number(lon);
  if (!Number.isFinite(longitude)) return "Longitude must be a number.";
  if (longitude < -180 || longitude > 180) return "Longitude must be between -180 and 180.";
  if (name.trim().length > 200) return "A place name is at most 200 characters.";
  return null;
}

/** A contact card needs a peer picked from the known-peers list. */
export function validateContact(peerKey: string): string | null {
  if (peerKey.trim() === "") return "Choose a peer to share.";
  return null;
}

export interface ComposerSendOptions {
  expiresIn?: number;
  sendIn?: number;
  pollOptions?: string[];
  /** The attached file is a recorded voice note, not a picked file. */
  voice?: boolean;
  /** Pin a place on the message: latitude -90..90, longitude -180..180. */
  location?: { lat: number; lon: number; name?: string };
  /** Share a peer's contact card: their name and destination hash. */
  contact?: { name: string; destination_hash: string };
  replyTo?: string;
}

export interface ComposerProps {
  /** Placeholder text, e.g. "Message Alice". */
  placeholder?: string;
  /** Message being replied to (shows a quote banner; wire id is sent). */
  replyTo?: ChatMessage | null;
  onCancelReply?: () => void;
  /** Message being edited (composer pre-fills; submit saves the edit). */
  editing?: ChatMessage | null;
  onCancelEdit?: () => void;
  disabled?: boolean;
  /** Polls are supported for this conversation; hide the toggle otherwise. */
  allowPoll?: boolean;
  /** Known peers for the contact picker; the Contact toggle hides when empty. */
  contactPeers?: { key: string; name: string }[];
  /** A saved draft to load into the box (set when its conversation opens). */
  draftSeed?: { key: string; text: string } | null;
  /** Park the current text as a draft for this conversation. */
  onSaveDraft?: (text: string) => Promise<void> | void;
  /** Stable key of the open conversation; the contact draft resets when it changes. */
  conversationKey?: string;
  onSend: (
    text: string,
    opts: ComposerSendOptions,
  ) => Promise<boolean | void> | boolean | void;
  onSendFile: (file: File, opts: Omit<ComposerSendOptions, "pollOptions">) => Promise<void> | void;
}

const EXPIRY_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "No expiry", seconds: undefined },
  { label: "5 minutes", seconds: 5 * 60 },
  { label: "1 hour", seconds: 3600 },
  { label: "1 day", seconds: 86400 },
  { label: "1 week", seconds: 7 * 86400 },
];

const SCHEDULE_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "Send now", seconds: undefined },
  { label: "In 5 minutes", seconds: 5 * 60 },
  { label: "In 1 hour", seconds: 3600 },
  { label: "Tomorrow", seconds: 86400 },
];

export default function Composer({
  placeholder = "Write a message…",
  replyTo,
  onCancelReply,
  editing,
  onCancelEdit,
  disabled = false,
  allowPoll = false,
  contactPeers,
  draftSeed,
  onSaveDraft,
  conversationKey,
  onSend,
  onSendFile,
}: ComposerProps) {
  const [text, setText] = useState("");
  const [mode, setMode] = useState<"message" | "poll" | "location" | "contact">("message");
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const [latitude, setLatitude] = useState("");
  const [longitude, setLongitude] = useState("");
  const [placeName, setPlaceName] = useState("");
  const [contactPeer, setContactPeer] = useState("");
  const [expiresIn, setExpiresIn] = useState<number | undefined>(undefined);
  const [sendIn, setSendIn] = useState<number | undefined>(undefined);
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  // A voice note is a recorded file, not a picked one: the marker
  // rides with the file into onSendFile so the receiver stores it.
  const [voiceFile, setVoiceFile] = useState(false);
  const [recording, setRecording] = useState(false);
  const [recordingSeconds, setRecordingSeconds] = useState(0);
  // The live recording, if any. A ref, not state: the stop callback and
  // the cleanup effects must see the latest value.
  const voiceSessionRef = useRef<VoiceSession | null>(null);
  // Set from the click until the recorder is running or has failed, so a
  // second click during the permission prompt cannot start another one.
  const startingRef = useRef(false);
  // Bumped whenever a recording must be abandoned (unmount, chat change),
  // so a permission prompt that resolves late does not start one.
  const voiceGenRef = useRef(0);
  const [savingDraft, setSavingDraft] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  // While recording, tick the elapsed counter once a second; the
  // interval dies with the recording (or the unmount).
  useEffect(() => {
    if (!recording) return;
    const timer = setInterval(() => setRecordingSeconds((s) => s + 1), 1000);
    return () => clearInterval(timer);
  }, [recording]);

  // Unmount: release the microphone whatever stage the recording is at.
  useEffect(() => {
    return () => {
      voiceGenRef.current += 1;
      const session = voiceSessionRef.current;
      voiceSessionRef.current = null;
      if (session) releaseVoiceSession(session);
    };
  }, []);

  // A recording belongs to the chat it began in: a chat change discards
  // it, so its output can never be attached to the newly open chat.
  useEffect(() => {
    voiceGenRef.current += 1;
    const session = voiceSessionRef.current;
    voiceSessionRef.current = null;
    if (session) {
      releaseVoiceSession(session);
      setRecording(false);
    }
  }, [conversationKey]);

  // A contact picked in one thread must not ride into the next:
  // the composer is reused across conversations, so reset contact
  // mode and the selected peer when the conversation changes. (The
  // text draft is per-conversation already, via draftSeed.) A completed
  // voice note from the previous chat is dropped the same way.
  const [contactConversationKey, setContactConversationKey] = useState<string | null>(null);
  if (conversationKey !== undefined && contactConversationKey !== conversationKey) {
    setContactConversationKey(conversationKey);
    setContactPeer("");
    setMode("message");
    if (voiceFile) {
      setFile(null);
      setVoiceFile(false);
    }
  }

  // Editing pre-fills the box; keep local edits once the user starts typing.
  const [editSeed, setEditSeed] = useState<number | null>(null);
  if (editing && editSeed !== editing.id) {
    setEditSeed(editing.id);
    setText(editing.text);
  }
  if (!editing && editSeed !== null) {
    setEditSeed(null);
    setText("");
  }

  // A saved draft pre-fills the box when its conversation opens — but
  // never clobbers text the user already typed into this composer.
  const [draftSeedKey, setDraftSeedKey] = useState<string | null>(null);
  if (draftSeed && draftSeedKey !== draftSeed.key) {
    setDraftSeedKey(draftSeed.key);
    if (text.trim() === "") setText(draftSeed.text);
  }
  if (!draftSeed && draftSeedKey !== null) {
    setDraftSeedKey(null);
  }

  async function handleSaveDraft() {
    if (!onSaveDraft || !text.trim() || savingDraft) return;
    setError(null);
    setSavingDraft(true);
    try {
      await onSaveDraft(text.trim());
      setText("");
    } catch (err) {
      setError(`Save draft failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSavingDraft(false);
    }
  }

  const canSend = !disabled && !sending && (mode === "poll" || mode === "location" || mode === "contact" ? true : text.trim().length > 0 || file !== null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (mode === "poll") {
      const problem = validatePoll(text, pollOptions);
      if (problem) return setError(problem);
      setSending(true);
      try {
        await onSend(text.trim(), {
          pollOptions: pollOptions.map((o) => o.trim()).filter(Boolean),
          expiresIn,
          sendIn,
        });
        setText("");
        setPollOptions(["", ""]);
        setMode("message");
      } finally {
        setSending(false);
      }
      return;
    }
    if (mode === "location") {
      const problem = validateLocation(latitude, longitude, placeName);
      if (problem) return setError(problem);
      const name = placeName.trim();
      setSending(true);
      try {
        await onSend(text.trim(), {
          location: {
            lat: Number(latitude),
            lon: Number(longitude),
            ...(name ? { name } : {}),
          },
          expiresIn,
          sendIn,
          replyTo: replyTo?.wire_id,
        });
        setText("");
        setLatitude("");
        setLongitude("");
        setPlaceName("");
        setMode("message");
      } finally {
        setSending(false);
      }
      return;
    }
    if (mode === "contact") {
      const problem = validateContact(contactPeer);
      if (problem) return setError(problem);
      const picked = contactPeers?.find((p) => p.key === contactPeer);
      if (!picked) return setError("Choose a peer to share.");
      setSending(true);
      try {
        const sent = await onSend(text.trim(), {
          contact: { name: picked.name, destination_hash: picked.key },
          expiresIn,
          sendIn,
          replyTo: replyTo?.wire_id,
        });
        // A failed share keeps the draft: the text and the picked
        // peer stay put so the user can retry instead of rebuilding
        // the card. An onSend that reports nothing is treated as a
        // failure — only a confirmed send clears the draft.
        if (!sent) return;
        setText("");
        setContactPeer("");
        setMode("message");
      } finally {
        setSending(false);
      }
      return;
    }
    if (file) {
      const problem = validateFile(file);
      if (problem) return setError(problem);
      setSending(true);
      try {
        await onSendFile(file, {
          expiresIn,
          sendIn,
          replyTo: replyTo?.wire_id,
          voice: voiceFile,
        });
        setFile(null);
        setVoiceFile(false);
        if (fileRef.current) fileRef.current.value = "";
      } finally {
        setSending(false);
      }
      return;
    }
    if (!text.trim()) return;
    setSending(true);
    try {
      await onSend(text.trim(), { expiresIn, sendIn, replyTo: replyTo?.wire_id });
      setText("");
    } finally {
      setSending(false);
    }
  }

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const picked = e.target.files?.[0] ?? null;
    if (!picked) {
      setFile(null);
      setVoiceFile(false);
      return;
    }
    const problem = validateFile(picked);
    if (problem) {
      setError(problem);
      setFile(null);
      setVoiceFile(false);
      e.target.value = "";
      return;
    }
    setError(null);
    setFile(picked);
    setVoiceFile(false);
  }

  async function startRecording() {
    if (recording || disabled || startingRef.current) return;
    startingRef.current = true;
    setError(null);
    const generation = voiceGenRef.current;
    let stream: MediaStream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    } catch (err) {
      startingRef.current = false;
      setError(
        `Microphone unavailable: ${err instanceof Error ? err.message : String(err)}`,
      );
      return;
    }
    // The composer unmounted or the chat changed while the permission
    // prompt was open: give the microphone straight back.
    if (generation !== voiceGenRef.current) {
      stream.getTracks().forEach((t) => t.stop());
      startingRef.current = false;
      return;
    }
    // Opus in WebM is the smallest widely supported recording; fall
    // back to the browser default when it is not offered.
    const mimeType = ["audio/webm;codecs=opus", "audio/webm"].find(
      (type) => MediaRecorder.isTypeSupported?.(type),
    );
    let recorder: MediaRecorder;
    try {
      recorder =
        mimeType !== undefined
          ? new MediaRecorder(stream, { mimeType })
          : new MediaRecorder(stream);
    } catch (err) {
      stream.getTracks().forEach((t) => t.stop());
      startingRef.current = false;
      setError(
        `Could not start recording: ${err instanceof Error ? err.message : String(err)}`,
      );
      return;
    }
    const session: VoiceSession = { recorder, stream, discard: false };
    const chunks: Blob[] = [];
    recorder.ondataavailable = (e) => {
      if (e.data.size > 0) chunks.push(e.data);
    };
    recorder.onstop = () => {
      stream.getTracks().forEach((t) => t.stop());
      if (voiceSessionRef.current === session) voiceSessionRef.current = null;
      // An abandoned recording (chat change, unmount, runtime error)
      // never becomes an attachment.
      if (session.discard) return;
      const type = recorder.mimeType || "audio/webm";
      const recorded = new File([new Blob(chunks, { type })], `voice-note-${Date.now()}.webm`, {
        type,
      });
      const problem = validateFile(recorded);
      if (problem) {
        setError(problem);
        setFile(null);
        setVoiceFile(false);
        return;
      }
      setError(null);
      setFile(recorded);
      setVoiceFile(true);
    };
    recorder.onerror = (e) => {
      if (session.discard) return;
      const reason = (e as { error?: { message?: string } }).error?.message ?? "unknown error";
      releaseVoiceSession(session);
      if (voiceSessionRef.current === session) voiceSessionRef.current = null;
      setRecording(false);
      setError(`Recording failed: ${reason}`);
    };
    voiceSessionRef.current = session;
    startingRef.current = false;
    setRecordingSeconds(0);
    setRecording(true);
    try {
      recorder.start();
    } catch (err) {
      releaseVoiceSession(session);
      if (voiceSessionRef.current === session) voiceSessionRef.current = null;
      setRecording(false);
      setError(
        `Could not start recording: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  function stopRecording() {
    const session = voiceSessionRef.current;
    if (session && session.recorder.state !== "inactive") session.recorder.stop();
    setRecording(false);
  }

  return (
    <form onSubmit={handleSubmit} className="border-t border-zinc-800 bg-zinc-950 px-3 py-2">
      {replyTo && (
        <div className="mb-2 flex items-center justify-between rounded-lg bg-zinc-900 px-3 py-1.5 text-sm">
          <span className="min-w-0 truncate text-zinc-400">
            Replying to <span className="text-zinc-200">{replyTo.peer_name || "message"}</span>
            : {replyTo.text.slice(0, 80)}
          </span>
          <button type="button" onClick={onCancelReply} aria-label="Cancel reply" className="ml-2 shrink-0 text-zinc-500 hover:text-zinc-200">
            ✕
          </button>
        </div>
      )}
      {editing && (
        <div className="mb-2 flex items-center justify-between rounded-lg bg-cyan-950 px-3 py-1.5 text-sm">
          <span className="text-cyan-200">Editing message — save to send the edit.</span>
          <button type="button" onClick={onCancelEdit} aria-label="Cancel edit" className="ml-2 shrink-0 text-cyan-400 hover:text-cyan-200">
            ✕
          </button>
        </div>
      )}

      {mode === "poll" ? (
        <div className="mb-2 flex flex-col gap-2">
          <label className="text-xs font-semibold text-zinc-400" htmlFor="poll-question">
            Poll question
          </label>
          <input
            id="poll-question"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Ask something…"
            disabled={disabled}
            className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          {pollOptions.map((option, i) => (
            <div key={i} className="flex items-center gap-2">
              <input
                value={option}
                onChange={(e) =>
                  setPollOptions((prev) => prev.map((o, j) => (j === i ? e.target.value : o)))
                }
                placeholder={`Option ${i + 1}`}
                aria-label={`Poll option ${i + 1}`}
                disabled={disabled}
                className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
              />
              {pollOptions.length > 2 && (
                <button
                  type="button"
                  aria-label={`Remove option ${i + 1}`}
                  onClick={() => setPollOptions((prev) => prev.filter((_, j) => j !== i))}
                  className="shrink-0 text-zinc-500 hover:text-zinc-200"
                >
                  ✕
                </button>
              )}
            </div>
          ))}
          {pollOptions.length < 10 && (
            <button
              type="button"
              onClick={() => setPollOptions((prev) => [...prev, ""])}
              className="self-start text-xs font-semibold text-cyan-400 hover:text-cyan-300"
            >
              + Add option
            </button>
          )}
        </div>
      ) : (
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void handleSubmit(e as unknown as React.FormEvent);
            }
          }}
          placeholder={placeholder}
          aria-label="Message"
          rows={2}
          disabled={disabled}
          className="w-full resize-none rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
        />
      )}

      {mode === "location" && (
        <div className="mb-2 flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <label className="w-20 shrink-0 text-xs font-semibold text-zinc-400" htmlFor="pin-latitude">
              Latitude
            </label>
            <input
              id="pin-latitude"
              value={latitude}
              onChange={(e) => setLatitude(e.target.value)}
              placeholder="34.05"
              inputMode="decimal"
              aria-label="Latitude"
              disabled={disabled}
              className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
            />
            <label className="w-20 shrink-0 text-xs font-semibold text-zinc-400" htmlFor="pin-longitude">
              Longitude
            </label>
            <input
              id="pin-longitude"
              value={longitude}
              onChange={(e) => setLongitude(e.target.value)}
              placeholder="-83.88"
              inputMode="decimal"
              aria-label="Longitude"
              disabled={disabled}
              className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
            />
          </div>
          <input
            value={placeName}
            onChange={(e) => setPlaceName(e.target.value)}
            placeholder="Place name (optional)"
            aria-label="Place name"
            disabled={disabled}
            className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
        </div>
      )}

      {mode === "contact" && (
        <div className="mb-2 flex flex-col gap-2">
          <label className="text-xs font-semibold text-zinc-400" htmlFor="contact-peer">
            Share a contact
          </label>
          <select
            id="contact-peer"
            value={contactPeer}
            onChange={(e) => setContactPeer(e.target.value)}
            aria-label="Peer to share"
            disabled={disabled}
            className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-500"
          >
            <option value="">Choose a peer…</option>
            {(contactPeers ?? []).map((p) => (
              <option key={p.key} value={p.key}>
                {p.name}
              </option>
            ))}
          </select>
          <p className="text-xs text-zinc-500">
            Shares their name and destination hash in this conversation. The card shows
            the sender&apos;s name for them — the recipient adds the peer from their own
            announce, so the name is only a claim until then.
          </p>
        </div>
      )}

      <div className="mt-2 flex flex-wrap items-center gap-2">
        <input
          ref={fileRef}
          type="file"
          aria-label="Attach a file"
          onChange={handleFileChange}
          disabled={disabled}
          className="hidden"
        />
        <button
          type="button"
          onClick={() => fileRef.current?.click()}
          disabled={disabled || recording}
          title="Attach a file (≤10 MiB)"
          className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-medium text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
        >
          📎 Attach
        </button>
        {canRecordVoice() &&
          (recording ? (
            <button
              type="button"
              onClick={stopRecording}
              aria-label="Stop recording"
              title="Stop recording and attach the voice note"
              className="rounded-full border border-red-500 bg-red-950 px-3 py-1 text-xs font-medium text-red-300 hover:bg-red-900"
            >
              ⏹ Stop ({formatVoiceDuration(recordingSeconds)})
            </button>
          ) : (
            <button
              type="button"
              onClick={() => void startRecording()}
              disabled={disabled}
              aria-label="Record a voice note"
              title="Record a voice note"
              className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-medium text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
            >
              🎤 Voice
            </button>
          ))}
        {allowPoll && (
          <button
            type="button"
            onClick={() => setMode((m) => (m === "poll" ? "message" : "poll"))}
            aria-pressed={mode === "poll"}
            className={`rounded-full border px-3 py-1 text-xs font-medium ${
              mode === "poll" ? "border-cyan-500 bg-cyan-950 text-cyan-300" : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
            }`}
          >
            📊 Poll
          </button>
        )}
        <button
          type="button"
          onClick={() => setMode((m) => (m === "location" ? "message" : "location"))}
          aria-pressed={mode === "location"}
          className={`rounded-full border px-3 py-1 text-xs font-medium ${
            mode === "location" ? "border-cyan-500 bg-cyan-950 text-cyan-300" : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
          }`}
        >
          📍 Pin
        </button>
        {contactPeers && contactPeers.length > 0 && (
          <button
            type="button"
            onClick={() => setMode((m) => (m === "contact" ? "message" : "contact"))}
            aria-pressed={mode === "contact"}
            className={`rounded-full border px-3 py-1 text-xs font-medium ${
              mode === "contact" ? "border-cyan-500 bg-cyan-950 text-cyan-300" : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
            }`}
          >
            👤 Contact
          </button>
        )}
        <label className="flex items-center gap-1 text-xs text-zinc-400">
          <span aria-hidden="true">⏱</span>
          <select
            aria-label="Disappearing message expiry"
            value={expiresIn === undefined ? "" : String(expiresIn)}
            onChange={(e) => setExpiresIn(e.target.value ? Number(e.target.value) : undefined)}
            disabled={disabled}
            className="rounded-md border border-zinc-700 bg-zinc-900 px-1.5 py-1 text-xs text-zinc-200 outline-none"
          >
            {EXPIRY_OPTIONS.map((o) => (
              <option key={o.label} value={o.seconds === undefined ? "" : o.seconds}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-1 text-xs text-zinc-400">
          <span aria-hidden="true">🕑</span>
          <select
            aria-label="Send later"
            value={sendIn === undefined ? "" : String(sendIn)}
            onChange={(e) => setSendIn(e.target.value ? Number(e.target.value) : undefined)}
            disabled={disabled}
            className="rounded-md border border-zinc-700 bg-zinc-900 px-1.5 py-1 text-xs text-zinc-200 outline-none"
          >
            {SCHEDULE_OPTIONS.map((o) => (
              <option key={o.label} value={o.seconds === undefined ? "" : o.seconds}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        {onSaveDraft && (
          <button
            type="button"
            onClick={() => void handleSaveDraft()}
            disabled={
              disabled ||
              savingDraft ||
              text.trim().length === 0 ||
              mode === "poll" ||
              mode === "contact" ||
              file !== null ||
              !!editing
            }
            title="Park this message as a draft to send later"
            className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-medium text-zinc-300 hover:bg-zinc-900 disabled:opacity-40"
          >
            Save draft
          </button>
        )}
        <button
          type="submit"
          disabled={!canSend}
          className="ml-auto rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
        >
          {sendIn !== undefined ? "Schedule" : editing ? "Save edit" : mode === "poll" ? "Post poll" : mode === "location" ? "Send pin" : mode === "contact" ? "Share contact" : "Send"}
        </button>
      </div>

      {file && (
        <p className="mt-2 text-xs text-zinc-400">
          {voiceFile ? "Voice note" : "Attached"}:{" "}
          <span className="text-zinc-200">{file.name}</span> ({formatBytes(file.size)})
          <button type="button" onClick={() => { setFile(null); setVoiceFile(false); if (fileRef.current) fileRef.current.value = ""; }} className="ml-2 text-zinc-500 hover:text-zinc-200">
            Remove
          </button>
        </p>
      )}
      {error && (
        <p role="alert" className="mt-2 text-xs text-red-400">
          {error}
        </p>
      )}
    </form>
  );
}
