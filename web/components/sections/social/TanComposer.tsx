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
// posts only, and it rides a scheduled post's queue entry too. A draft
// keeps all three attachments: saving one passes the picked image and
// card (and a place the draft already carries) through, and loading a
// draft prefills them, so a half-written post publishes exactly what
// was drafted. Max length is a client-side guard only.

"use client";

import { useEffect, useRef, useState } from "react";
import { validateContact, validatePoll } from "@/components/chat/Composer";
import {
  TAN_MAX_IMAGE_BYTES,
  readFileAsDataUrl,
  type PublishTanOptions,
} from "@/lib/api";
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
  /** Image a loaded draft carries, as a data URL with its file name
   *  and size — re-sent unchanged until a new file is picked. */
  initialImage?: { dataUrl: string; name: string; size: number } | null;
  /** Contact card a loaded draft carries. */
  initialContact?: { name: string; destination_hash: string } | null;
  /** Place a loaded draft carries. The composer cannot pick a place —
   *  it only preserves one a draft already has, through save and publish. */
  initialLocation?: { lat: number; lon: number; name?: string } | null;
  /** Park the current text as a draft instead of posting it. */
  onSaveDraft?: (
    text: string,
    opts: PublishTanOptions,
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
  initialImage,
  initialContact,
  initialLocation,
  onSaveDraft,
  contactPeers,
}: TanComposerProps) {
  const [text, setText] = useState(initialText);
  const [mode, setMode] = useState<"post" | "poll">("post");
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const [expiresIn, setExpiresIn] = useState<number | undefined>(initialExpiresIn);
  const [sendIn, setSendIn] = useState<number | undefined>(undefined);
  const [file, setFile] = useState<File | null>(null);
  // A loaded draft's image, as a data URL: a newly picked file wins,
  // but the draft's image survives until one is picked or it is removed.
  const [imageDataUrl, setImageDataUrl] = useState<{
    dataUrl: string;
    name: string;
    size: number;
  } | null>(initialImage ?? null);
  // A loaded draft's card, when its peer is still in the known list:
  // prefilled in the picker, so it can be changed like a fresh pick.
  const [contactPeer, setContactPeer] = useState(
    initialContact &&
      (contactPeers ?? []).some((p) => p.key === initialContact.destination_hash)
      ? initialContact.destination_hash
      : "",
  );
  // A loaded draft's card, when its peer is no longer listed: shown
  // and carried through unchanged, never re-picked.
  const [attachedContact, setAttachedContact] = useState<{
    name: string;
    destination_hash: string;
  } | null>(
    initialContact &&
      !(contactPeers ?? []).some(
        (p) => p.key === initialContact.destination_hash,
      )
      ? initialContact
      : null,
  );
  // The card a loaded draft carried, retained independently of the
  // live picker list: if the selected peer disappears from the list
  // before submit, the draft's original card is carried instead of
  // failing the picker lookup. Cleared when the user changes or
  // removes the selection.
  const [loadedContact, setLoadedContact] = useState<{
    name: string;
    destination_hash: string;
  } | null>(initialContact ?? null);
  // A loaded draft's place: shown and carried through unchanged.
  const [location, setLocation] = useState<{
    lat: number;
    lon: number;
    name?: string;
  } | null>(initialLocation ?? null);
  const [showContact, setShowContact] = useState(
    initialContact !== null &&
      initialContact !== undefined &&
      (contactPeers ?? []).some(
        (p) => p.key === initialContact.destination_hash,
      ),
  );
  const [error, setError] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  // Replies cannot carry an image: drop a picked file the moment the
  // composer becomes a reply, instead of failing on submit.
  useEffect(() => {
    if (replyTo) {
      setFile(null);
      setImageDataUrl(null);
      if (fileRef.current) fileRef.current.value = "";
      setContactPeer("");
      setAttachedContact(null);
      setLoadedContact(null);
      setLocation(null);
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
    setImageDataUrl(null);
    setContactPeer("");
    setAttachedContact(null);
    setLoadedContact(null);
    setLocation(null);
    setShowContact(false);
    if (fileRef.current) fileRef.current.value = "";
  }

  // The image to carry: a newly picked file, read as a data URL, or a
  // loaded draft's image, re-sent as the data URL it was loaded with.
  async function readAttachedImage(): Promise<{ dataUrl: string } | undefined> {
    if (file) return { dataUrl: await readFileAsDataUrl(file) };
    return imageDataUrl ? { dataUrl: imageDataUrl.dataUrl } : undefined;
  }

  // The card to carry: a newly picked peer, or a loaded draft's card,
  // carried through unchanged when its peer is no longer listed — or
  // when its peer disappears from the live list after the draft loaded.
  // A fresh pick has no retained card, so a vanished fresh pick still
  // fails rather than posting without the card.
  function pickedContact(): { name: string; destination_hash: string } | undefined {
    if (!contactPeer) return attachedContact ?? undefined;
    const picked = (contactPeers ?? []).find((p) => p.key === contactPeer);
    if (picked) return { name: picked.name, destination_hash: picked.key };
    if (loadedContact?.destination_hash === contactPeer) return loadedContact;
    throw new Error("Choose a peer to share.");
  }

  async function handleSaveDraft() {
    if (!onSaveDraft) return;
    const problem = validateTanText(text);
    if (problem) return setError(problem);
    setError(null);
    try {
      const image = await readAttachedImage();
      const contact = pickedContact();
      await onSaveDraft(text.trim(), {
        replyTo: replyTo ?? undefined,
        expiresIn,
        image,
        location: location ?? undefined,
        contact,
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
    // fail here rather than post without the card, unless it is the
    // loaded draft's card, which pickedContact() retains.
    let contact: { name: string; destination_hash: string } | undefined;
    if (contactPeer) {
      const contactProblem = validateContact(contactPeer);
      if (contactProblem) return setError(contactProblem);
      try {
        contact = pickedContact();
      } catch {
        return setError("Choose a peer to share.");
      }
    } else {
      // A loaded draft's card, carried through unchanged.
      contact = attachedContact ?? undefined;
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
      } else if (imageDataUrl) {
        image = { dataUrl: imageDataUrl.dataUrl };
      }
      if (sendIn !== undefined) {
        await composer.schedule(text.trim(), sendIn, { replyTo: replyTo ?? undefined, expiresIn, image, location: location ?? undefined, contact });
      } else {
        await composer.publish(text.trim(), {
          replyTo: replyTo ?? undefined,
          expiresIn,
          image,
          location: location ?? undefined,
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
            onChange={(e) => {
              const next = e.target.value;
              setContactPeer(next);
              // Picking a different peer drops the loaded draft's card:
              // only the draft's own card survives its peer disappearing.
              if (loadedContact && next !== loadedContact.destination_hash) {
                setLoadedContact(null);
              }
            }}
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
            {loadedContact &&
              !(contactPeers ?? []).some(
                (p) => p.key === loadedContact.destination_hash,
              ) && (
                <option
                  key={loadedContact.destination_hash}
                  value={loadedContact.destination_hash}
                >
                  {loadedContact.name}
                </option>
              )}
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
            // A newly picked file replaces a loaded draft's image.
            if (picked) setImageDataUrl(null);
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
            // Polls cannot carry an image, a place or a contact card:
            // drop a picked file, peer or place rather than silently
            // posting the poll without it.
            if (mode === "post") setFile(null);
            if (mode === "post") setImageDataUrl(null);
            if (mode === "post") setLocation(null);
            if (mode === "post") {
              setContactPeer("");
              setAttachedContact(null);
              setLoadedContact(null);
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
                if (prev) {
                  setContactPeer("");
                  setLoadedContact(null);
                }
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

      {(file || imageDataUrl) && (
        <p className="mt-2 text-xs text-zinc-400">
          Attached:{" "}
          <span className="text-zinc-200">
            {file ? file.name : imageDataUrl?.name}
          </span>{" "}
          <span className="text-zinc-500">
            (
            {Math.max(
              1,
              Math.round((file?.size ?? imageDataUrl?.size ?? 0) / 1024),
            )}{" "}
            KB)
          </span>
          <button
            type="button"
            onClick={() => {
              setFile(null);
              setImageDataUrl(null);
              if (fileRef.current) fileRef.current.value = "";
            }}
            className="ml-2 text-zinc-500 hover:text-zinc-200"
          >
            Remove
          </button>
        </p>
      )}
      {location && (
        <p className="mt-2 text-xs text-zinc-400">
          Place:{" "}
          <span className="text-zinc-200">
            {location.name ?? `${location.lat}, ${location.lon}`}
          </span>
          <button
            type="button"
            onClick={() => setLocation(null)}
            className="ml-2 text-zinc-500 hover:text-zinc-200"
          >
            Remove
          </button>
        </p>
      )}
      {(contactPeer || attachedContact) && (
        <p className="mt-2 text-xs text-zinc-400">
          Sharing:{" "}
          <span className="text-zinc-200">
            {pickedContactPeer?.displayName ??
              pickedContactPeer?.name ??
              loadedContact?.name ??
              attachedContact?.name ??
              contactPeer}
          </span>
          <button
            type="button"
            onClick={() => {
              setContactPeer("");
              setAttachedContact(null);
              setLoadedContact(null);
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
