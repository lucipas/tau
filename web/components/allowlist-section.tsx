"use client";

import { useEffect, useRef, useState } from "react";

import { peerLabel } from "@/lib/format";
import type { AllowlistResponse } from "@/lib/types";

import { SectionLabel } from "./ui";

/** The DM allow-list: who may message this node when it is enforced. */
export function AllowlistSection({
  allowlist,
  onAdd,
  onRemove,
  onToggle,
}: {
  allowlist: AllowlistResponse | null;
  onAdd: (target: string) => Promise<unknown>;
  onRemove: (target: string) => Promise<unknown>;
  onToggle: (enabled: boolean) => Promise<unknown>;
}) {
  const [target, setTarget] = useState("");
  const [adding, setAdding] = useState(false);
  const [removing, setRemoving] = useState<string | null>(null);
  const [toggling, setToggling] = useState(false);
  const [confirmingEnable, setConfirmingEnable] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
    };
  }, []);

  async function handleAdd() {
    const value = target.trim();
    if (!value || adding) return;
    setAdding(true);
    setError(null);
    try {
      await onAdd(value);
      setTarget("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "add failed");
    } finally {
      setAdding(false);
    }
  }

  async function handleRemove(destination: string) {
    if (removing) return;
    setRemoving(destination);
    setError(null);
    try {
      await onRemove(destination);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "remove failed");
    } finally {
      setRemoving(null);
    }
  }

  async function handleToggle() {
    if (toggling) return;
    // Enforcing an empty list locks every sender out, so it needs an
    // explicit second click — the same two-step pattern as cancelling
    // a queued message.
    if (
      allowlist !== null &&
      !allowlist.enabled &&
      allowlist.entries.length === 0 &&
      !confirmingEnable
    ) {
      setConfirmingEnable(true);
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
      confirmTimer.current = setTimeout(() => setConfirmingEnable(false), 5000);
      return;
    }
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    setConfirmingEnable(false);
    setToggling(true);
    setError(null);
    try {
      await onToggle(!(allowlist?.enabled ?? false));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "toggle failed");
    } finally {
      setToggling(false);
    }
  }

  const enabled = allowlist?.enabled ?? false;
  const entries = allowlist?.entries ?? [];

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>dm allow-list</SectionLabel>
        <button
          type="button"
          disabled={toggling || allowlist === null}
          onClick={() => {
            void handleToggle();
          }}
          className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 font-mono text-[10px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
            confirmingEnable
              ? "border-amber/60 bg-amber/10 text-amber"
              : enabled
                ? "border-teal-400/40 bg-teal-400/10 text-teal-300"
                : "border-line/80 bg-panel/60 text-mist hover:border-teal-400/50 hover:text-teal-300"
          }`}
        >
          {toggling
            ? "saving…"
            : confirmingEnable
              ? "confirm — nobody can DM"
              : enabled
                ? "enforced"
                : "off"}
        </button>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        {allowlist === null
          ? "loading…"
          : enabled
            ? "only listed senders can DM this node — everyone else is dropped."
            : "anyone can DM this node. Turn enforcement on to restrict it to the list."}
      </p>

      {confirmingEnable ? (
        <p className="mt-2 font-mono text-[11px] leading-relaxed text-amber">
          the list is empty — enforcing it means nobody can DM this node.
          Click again to confirm.
        </p>
      ) : null}

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      {entries.length > 0 ? (
        <ul className="mt-3 flex flex-col gap-2">
          {entries.map((entry) => (
            <li
              key={entry.destination}
              className="flex items-center justify-between gap-3 rounded-xl border border-line/70 bg-void/50 px-3 py-2"
            >
              <span className="min-w-0 truncate font-mono text-[12px] text-zinc-100">
                {peerLabel(entry.name, entry.destination)}
              </span>
              <button
                type="button"
                disabled={removing === entry.destination}
                onClick={() => {
                  void handleRemove(entry.destination);
                }}
                className="shrink-0 rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-rose-400/50 hover:text-rose-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
              >
                {removing === entry.destination ? "removing…" : "remove"}
              </button>
            </li>
          ))}
        </ul>
      ) : allowlist !== null ? (
        <p className="mt-3 font-mono text-[11px] text-mist/60">
          no senders listed.
        </p>
      ) : null}

      <form
        className="mt-3 flex gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          void handleAdd();
        }}
      >
        <input
          value={target}
          onChange={(event) => setTarget(event.target.value)}
          placeholder="peer name, hash or prefix"
          aria-label="peer to allow"
          className="min-w-0 flex-1 rounded-md border border-line/80 bg-void/60 px-2 py-1.5 font-mono text-[12px] text-zinc-100 placeholder:text-mist/50 focus:border-signal/60 focus:outline-none"
        />
        <button
          type="submit"
          disabled={adding || target.trim() === ""}
          className="shrink-0 rounded-md border border-line/80 bg-panel/60 px-3 py-1.5 font-mono text-[11px] text-mist transition hover:border-teal-400/50 hover:text-teal-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
        >
          {adding ? "adding…" : "allow"}
        </button>
      </form>
    </section>
  );
}
