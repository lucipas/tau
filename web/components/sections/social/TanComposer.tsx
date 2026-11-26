// TanComposer.tsx — the Social composer: text post, poll mode, schedule
// (send later), disappearing expiry, image attach, and contact-card attach.
//
// An attached image is read as a base64 data URL and published with the
// post (the bridge validates and stores it); replies and polls cannot
// carry an image, so the picker is hidden for those and the submit
// guards them. A scheduled post can carry one — it rides the queue
// entry until the publish pass attaches it. Polls can be scheduled
// like posts, but a scheduled poll cannot carry an image. A contact
// card works the same way: picked from the known-peers list, top-level
// posts only, and it rides a scheduled post's queue entry too. Max length
// is a client-side guard only.

"use client";

import { useEffect, useRef, useState } from "react";
import { validateContact, validatePoll } from "@/components/chat/Composer";
import { TAN_MAX_IMAGE_BYTES, readFileAsDataUrl } from "@/lib/api";
import type { UseTanComposerResult } from "@/lib/use-tan";

export const TAN_MAX_CHARS = 2000;

export function validateTanText(text: string): string | null {
  if (!text.trim()) return "Write something first.";
  if (text.length > TAN_MAX_CHARS) {
    return `That post is ${text.length} characters — the limit is ${TAN_MAX_CHARS}.`;
  }
  return null;
}

const EXPIRY_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "No expiry", seconds: undefined },
  { label: "1 hour", seconds: 3600 },
  { label: "1 day", seconds: 86400 },
  { label: "7 days", seconds: 7 * 86400 },
];

export const SCHEDULE_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "Post now", seconds: undefined },
  { label: "In 5 minutes", seconds: 5 * 60 },
  { label: "In 1 hour", seconds: 3600 },
  { label: "Tomorrow", seconds: 86400 },
];

export interface TanComposerProps {
  composer: UseTanComposerResult;
  /** Called after a successful publish / schedule / poll. */
  onPosted?: () => void;
  /** Hash of the post being replied to, when composing a reply. */
  replyTo?: string | null;
  replyToName?: string;
  onCancelReply?: () => void;
  placeholder?: string;
  /** Text to start from — a loaded draft. The parent remounts the
   *  composer (via `key`) when a different draft is loaded. */
  initialText?: string;
  /** Expiry a loaded draft carries, in seconds. */
  initialExpiresIn?: number;
  /** Park the current text as a draft instead of posting it. */
  onSaveDraft?: (
    text: string,
    opts: { replyTo?: string; expiresIn?: number },
  ) => Promise<void>;
  /** Known peers for the contact picker; the Contact toggle hides when empty. */
  // `name` is the peer's announced name — the one the signed card
  // carries. `displayName` is the owner's local alias when one exists,
  // shown in the picker only and never submitted.
  contactPeers?: { key: string; name: string; displayName?: string }[];
}

export default function TanComposer({
  composer,
  onPosted,
  replyTo,
  replyToName,
  onCancelReply,
  placeholder = "What's happening on the mesh?",
  initialText = "",
  initialExpiresIn,
  onSaveDraft,
  contactPeers,
}: TanComposerProps) {
  const [text, setText] = useState(initialText);
  const [mode, setMode] = useState<"post" | "poll">("post");
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const [expiresIn, setExpiresIn] = useState<number | undefined>(initialExpiresIn);
  const [sendIn, setSendIn] = useState<number | undefined>(undefined);
  const [file, setFile] = useState<File | null>(null);
  const [contactPeer, setContactPeer] = useState("");
  const [showContact, setShowContact] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  // Replies cannot carry an image: drop a picked file the moment the
  // composer becomes a reply, instead of failing on submit.
  useEffect(() => {
    if (replyTo) {
      setFile(null);
      if (fileRef.current) fileRef.current.value = "";
      setContactPeer("");
      setShowContact(false);
    }
  }, [replyTo]);

  function reset() {
    setText("");
    setPollOptions(["", ""]);
    setMode("post");
    setExpiresIn(undefined);
    setSendIn(undefined);
    setFile(null);
    setContactPeer("");
    setShowContact(false);
    if (fileRef.current) fileRef.current.value = "";
  }

  async function handleSaveDraft() {
    if (!onSaveDraft) return;
    const problem = validateTanText(text);
    if (problem) return setError(problem);
    setError(null);
    try {
      await onSaveDraft(text.trim(), {
        replyTo: replyTo ?? undefined,
        expiresIn,
      });
      reset();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Saving the draft failed.");
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (mode === "poll") {
      const problem = validatePoll(text, pollOptions);
      if (problem) return setError(problem);
      try {
        if (sendIn !== undefined) {
          await composer.schedulePoll(
            text.trim(),
            pollOptions.map((o) => o.trim()).filter(Boolean),
            sendIn,
            { expiresIn },
          );
        } else {
          await composer.poll(
            text.trim(),
            pollOptions.map((o) => o.trim()).filter(Boolean),
            expiresIn,
          );
        }
        reset();
        onPosted?.();
      } catch {
        setError(composer.error ?? "Posting the poll failed.");
      }
      return;
    }
    const problem = validateTanText(text);
    if (problem) return setError(problem);
    if (file && file.size > TAN_MAX_IMAGE_BYTES) {
      return setError("That image is larger than the 2 MB limit.");
    }
    // Replies and polls cannot carry a card: the picker is hidden for
    // those, so a pick here is always a top-level post — but a picked
    // peer that vanished from the list (or a stale state) must still
    // fail here rather than post without the card.
    let contact: { name: string; destination_hash: string } | undefined;
    if (contactPeer) {
      const contactProblem = validateContact(contactPeer);
      if (contactProblem) return setError(contactProblem);
      const picked = (contactPeers ?? []).find((p) => p.key === contactPeer);
      if (!picked) return setError("Choose a peer to share.");
      contact = { name: picked.name, destination_hash: picked.key };
    }
    try {
      let image: { dataUrl: string } | undefined;
      if (file) {
        try {
          image = { dataUrl: await readFileAsDataUrl(file) };
        } catch (err) {
          return setError(
            err instanceof Error ? err.message : "Could not read the image file.",
          );
        }
      }
      if (sendIn !== undefined) {
        await composer.schedule(text.trim(), sendIn, { replyTo: replyTo ?? undefined, expiresIn, image, contact });
      } else {
        await composer.publish(text.trim(), {
          replyTo: replyTo ?? undefined,
          expiresIn,
          image,
          contact,
        });
      }
      reset();
      onPosted?.();
    } catch {
      setError(composer.error ?? "Posting failed.");
    }
  }

  const overLimit = text.length > TAN_MAX_CHARS;

  // The picked peer, for the "Sharing:" line: the alias (when one
  // exists) is what the owner recognizes, while the signed card
  // carries the announced name.
  const pickedContactPeer = contactPeer
    ? (contactPeers ?? []).find((p) => p.key === contactPeer)
    : undefined;

  return (
    <form
      onSubmit={handleSubmit}
      className="border-b border-zinc-800 px-3 py-3 sm:px-4"
      aria-label="Compose a Tan post"
    >
      {replyTo && (
        <div className="mb-2 flex items-center justify-between rounded-lg bg-zinc-900 px-3 py-1.5 text-sm">
          <span className="min-w-0 truncate text-zinc-400">
            Replying to <span className="text-zinc-200">{replyToName ?? "post"}</span>
          </span>
          <button
            type="button"
            onClick={onCancelReply}
            aria-label="Cancel reply"
            className="ml-2 shrink-0 text-zinc-500 hover:text-zinc-200"
          >
            ✕
          </button>
        </div>
      )}
      {mode === "poll" ? (
        <div className="flex flex-col gap-2">
          <label className="text-xs font-semibold text-zinc-400" htmlFor="tan-poll-question">
            Poll question
          </label>
          <input
            id="tan-poll-question"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Ask something…"
            disabled={composer.busy}
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
                disabled={composer.busy}
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
          placeholder={placeholder}
          aria-label="Compose a post"
          rows={3}
          disabled={composer.busy}
          className="w-full resize-none rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
        />
      )}

      {mode === "post" && !replyTo && showContact && (contactPeers ?? []).length > 0 && (
        <div className="mt-2 flex flex-col gap-2">
          <label className="text-xs font-semibold text-zinc-400" htmlFor="tan-contact-peer">
            Share a contact
          </label>
          <select
            id="tan-contact-peer"
            value={contactPeer}
            onChange={(e) => setContactPeer(e.target.value)}
            aria-label="Peer to share"
            disabled={composer.busy}
            className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-500"
          >
            <option value="">Choose a peer…</option>
            {(contactPeers ?? []).map((p) => (
              <option key={p.key} value={p.key}>
                {p.displayName ?? p.name}
              </option>
            ))}
          </select>
          <p className="text-xs text-zinc-500">
            Shares their name and destination hash on the post. Readers add
            the peer from their own announce, so the name is only a claim
            until then.
          </p>
        </div>
      )}

      <div className="mt-2 flex flex-wrap items-center gap-2">
        <input
          ref={fileRef}
          type="file"
          accept="image/*"
          aria-label="Attach an image"
          disabled={composer.busy}
          className="hidden"
          onChange={(e) => {
            const picked = e.target.files?.[0] ?? null;
            setFile(picked);
            if (picked) setError(null);
          }}
        />
        <button
          type="button"
          onClick={() => fileRef.current?.click()}
          disabled={composer.busy || mode === "poll" || !!replyTo}
          title={
            replyTo
              ? "Replies can't carry an image"
              : mode === "poll"
                ? "Polls can't carry an image"
                : "Attach an image"
          }
          className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-medium text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
        >
          🖼 Attach
        </button>
        <button
          type="button"
          onClick={() => {
            // Polls cannot carry an image or a contact card: drop a
            // picked file or peer rather than silently posting the
            // poll without it.
            if (mode === "post") setFile(null);
            if (mode === "post") {
              setContactPeer("");
              setShowContact(false);
            }
            setMode(mode === "poll" ? "post" : "poll");
          }}
          aria-pressed={mode === "poll"}
          disabled={composer.busy}
          className={`rounded-full border px-3 py-1 text-xs font-medium ${
            mode === "poll"
              ? "border-cyan-500 bg-cyan-950 text-cyan-300"
              : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
          }`}
        >
          📊 Poll
        </button>
        {(contactPeers ?? []).length > 0 && (
          <button
            type="button"
            onClick={() => {
              // Opening the picker on a poll, or a reply, would offer
              // a card the post cannot carry — keep it closed there.
              if (mode === "poll" || replyTo) return;
              setShowContact((prev) => {
                if (prev) setContactPeer("");
                return !prev;
              });
            }}
            aria-pressed={showContact}
            disabled={composer.busy || mode === "poll" || !!replyTo}
            title={
              replyTo
                ? "Replies can't carry a contact card"
                : mode === "poll"
                  ? "Polls can't carry a contact card"
                  : "Share a peer's contact card"
            }
            className={`rounded-full border px-3 py-1 text-xs font-medium ${
              showContact
                ? "border-cyan-500 bg-cyan-950 text-cyan-300"
                : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
            } disabled:opacity-50`}
          >
            👤 Contact
          </button>
        )}
        <label className="flex items-center gap-1 text-xs text-zinc-400">
          <span aria-hidden="true">⏱</span>
          <select
            aria-label="Post expiry"
            value={expiresIn === undefined ? "" : String(expiresIn)}
            onChange={(e) => setExpiresIn(e.target.value ? Number(e.target.value) : undefined)}
            disabled={composer.busy}
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
            disabled={composer.busy}
            className="rounded-md border border-zinc-700 bg-zinc-900 px-1.5 py-1 text-xs text-zinc-200 outline-none"
          >
            {SCHEDULE_OPTIONS.map((o) => (
              <option key={o.label} value={o.seconds === undefined ? "" : o.seconds}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <span
          className={`ml-auto text-xs ${overLimit ? "font-semibold text-red-400" : "text-zinc-500"}`}
        >
          {text.length}/{TAN_MAX_CHARS}
        </span>
        {onSaveDraft && (
          <button
            type="button"
            onClick={() => void handleSaveDraft()}
            disabled={composer.busy || mode === "poll" || !text.trim()}
            title={
              mode === "poll"
                ? "Polls can't be saved as drafts"
                : "Save this post as a draft to finish later"
            }
            className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-medium text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
          >
            Save draft
          </button>
        )}
        <button
          type="submit"
          disabled={composer.busy || (mode === "post" && !text.trim())}
          className="rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
        >
          {composer.busy
            ? "Posting…"
            : sendIn !== undefined
              ? "Schedule"
              : mode === "poll"
                ? "Post poll"
                : "Post"}
        </button>
      </div>

      {file && (
        <p className="mt-2 text-xs text-zinc-400">
          Attached: <span className="text-zinc-200">{file.name}</span>{" "}
          <span className="text-zinc-500">({Math.max(1, Math.round(file.size / 1024))} KB)</span>
          <button
            type="button"
            onClick={() => {
              setFile(null);
              if (fileRef.current) fileRef.current.value = "";
            }}
            className="ml-2 text-zinc-500 hover:text-zinc-200"
          >
            Remove
          </button>
        </p>
      )}
      {contactPeer && (
        <p className="mt-2 text-xs text-zinc-400">
          Sharing:{" "}
          <span className="text-zinc-200">
            {pickedContactPeer?.displayName ?? pickedContactPeer?.name ?? contactPeer}
          </span>
          <button
            type="button"
            onClick={() => setContactPeer("")}
            className="ml-2 text-zinc-500 hover:text-zinc-200"
          >
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
