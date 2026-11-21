// NewGroupPanel.tsx — create a group from the console: a name plus a
// member picker over known peers. The server adds this node to the
// roster itself, so the picker only needs the other members.

"use client";

import { useState } from "react";
import type { Group, Peer } from "@/lib/types";

export interface NewGroupPanelProps {
  peers: Peer[];
  onCreate: (name: string, members: string[]) => Promise<Group>;
}

export default function NewGroupPanel({ peers, onCreate }: NewGroupPanelProps) {
  const [name, setName] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [formError, setFormError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  function toggleMember(hash: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(hash)) {
        next.delete(hash);
      } else {
        next.add(hash);
      }
      return next;
    });
  }

  async function handleCreate() {
    const trimmed = name.trim();
    if (!trimmed) {
      setFormError("Give the group a name.");
      return;
    }
    // A peer can drop out of the registry while the form is open; its
    // hash must not be submitted, and it has no checkbox to uncheck.
    const available = new Set(peers.map((p) => p.destination));
    const members = [...selected].filter((hash) => available.has(hash));
    if (members.length === 0) {
      setFormError("Pick at least one peer.");
      return;
    }
    setCreating(true);
    setFormError(null);
    try {
      await onCreate(trimmed, members);
      setName("");
      setSelected(new Set());
    } catch (err) {
      setFormError(err instanceof Error ? err.message : String(err));
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="border-b border-zinc-800 px-3 py-2">
      <label
        className="text-xs font-semibold text-zinc-400"
        htmlFor="new-group-name"
      >
        Group name
      </label>
      <div className="mt-1 flex flex-col gap-2">
        <input
          id="new-group-name"
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void handleCreate();
          }}
          placeholder="Weekend mesh crew"
          aria-label="Group name"
          className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
        />
        {peers.length === 0 ? (
          <p className="text-xs text-zinc-500">
            No known peers yet — a peer has to announce before they can join a group.
          </p>
        ) : (
          <ul className="flex max-h-32 flex-col gap-1 overflow-y-auto">
            {peers.map((peer) => (
              <li key={peer.destination}>
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-200">
                  <input
                    type="checkbox"
                    checked={selected.has(peer.destination)}
                    onChange={() => toggleMember(peer.destination)}
                  />
                  <span className="min-w-0 truncate">{peer.name}</span>
                </label>
              </li>
            ))}
          </ul>
        )}
        {formError && (
          <p role="alert" className="text-xs text-red-400">
            {formError}
          </p>
        )}
        <button
          type="button"
          disabled={creating}
          onClick={() => void handleCreate()}
          className="rounded-full bg-cyan-500 px-3 py-1.5 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
        >
          {creating ? "Creating…" : "Create group"}
        </button>
      </div>
    </div>
  );
}
