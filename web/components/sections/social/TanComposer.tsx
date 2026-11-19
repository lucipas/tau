// TanComposer.tsx — the Social composer: text post, poll mode, schedule
// (send later), disappearing expiry, and image attach.
//
// An attached image is read as a base64 data URL and published with the
// post (the bridge validates and stores it); replies and polls cannot
// carry an image, so the picker is hidden for those and the submit
// guards them. A scheduled post can carry one — it rides the queue
// entry until the publish pass attaches it. Polls can be scheduled
// like posts, but a scheduled poll cannot carry an image. Max length
// is a client-side guard only.

"use client";

import { useEffect, useRef, useState } from "react";
import { validatePoll } from "@/components/chat/Composer";
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

const SCHEDULE_OPTIONS: { label: string; seconds: number | undefined }[] = [
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
}: TanComposerProps) {
  const [text, setText] = useState(initialText);
  const [mode, setMode] = useState<"post" | "poll">("post");
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const [expiresIn, setExpiresIn] = useState<number | undefined>(initialExpiresIn);
  const [sendIn, setSendIn] = useState<number | undefined>(undefined);
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  // Replies cannot carry an image: drop a picked file the moment the
  // composer becomes a reply, instead of failing on submit.
  useEffect(() => {
    if (replyTo) {
      setFile(null);
      if (fileRef.current) fileRef.current.value = "";
    }
  }, [replyTo]);

  function reset() {
    setText("");
    setPollOptions(["", ""]);
    setMode("post");
    setExpiresIn(undefined);
    setSendIn(undefined);
    setFile(null);
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
        await composer.schedule(text.trim(), sendIn, { replyTo: replyTo ?? undefined, expiresIn, image });
      } else {
        await composer.publish(text.trim(), {
          replyTo: replyTo ?? undefined,
          expiresIn,
          image,
        });
      }
      reset();
      onPosted?.();
    } catch {
      setError(composer.error ?? "Posting failed.");
    }
  }

  const overLimit = text.length > TAN_MAX_CHARS;

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
            // Polls cannot carry an image: drop a picked file rather
            // than silently posting the poll without it.
            if (mode === "post") setFile(null);
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
      {error && (
        <p role="alert" className="mt-2 text-xs text-red-400">
          {error}
        </p>
      )}
    </form>
  );
}
