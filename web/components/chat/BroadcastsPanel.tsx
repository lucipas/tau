// BroadcastsPanel.tsx — saved broadcast lists: create, send, rename, delete.
// A broadcast is a named set of peers; a send fans out as one private
// DM per member, so recipients never see each other or the list.

"use client";

import { useState } from "react";
import BeaconControl from "./BeaconControl";
import type {
  Beacon,
  BroadcastList,
  BroadcastSendResponse,
  Peer,
} from "@/lib/types";

export interface BroadcastsPanelProps {
  broadcasts: BroadcastList[];
  peers: Peer[];
  loading?: boolean;
  error?: string | null;
  onCreate: (name: string, members: string[]) => Promise<void>;
  onDelete: (name: string) => Promise<void>;
  // Resolves to the name the server stored, which can differ from the
  // requested one when it normalises the name (collapsed spaces, length
  // cap); drafts and send results are re-keyed under that name.
  onRename: (name: string, newName: string) => Promise<string>;
  onSend: (name: string, text: string) => Promise<BroadcastSendResponse>;
  /** Live beacons, so a list with a running beacon shows its banner. */
  beacons?: Beacon[];
  onRefreshBeacons?: () => Promise<void> | void;
  onBeaconError?: (message: string) => void;
}

export default function BroadcastsPanel({
  broadcasts,
  peers,
  loading = false,
  error,
  onCreate,
  onDelete,
  onRename,
  onSend,
  beacons = [],
  onRefreshBeacons,
  onBeaconError,
}: BroadcastsPanelProps) {
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [formError, setFormError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [texts, setTexts] = useState<Record<string, string>>({});
  const [results, setResults] = useState<
    Record<string, BroadcastSendResponse>
  >({});
  const [sending, setSending] = useState<Set<string>>(new Set());
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [renameName, setRenameName] = useState("");
  const [panelError, setPanelError] = useState<string | null>(null);
  // The list whose beacon control is open. A list with a running
  // beacon always shows its control (so it can be stopped); other
  // lists show it only once expanded, so fifty lists do not mount
  // fifty ticking controls.
  const [sharing, setSharing] = useState<string | null>(null);

  const count = broadcasts.length;

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
      setFormError("Give the list a name.");
      return;
    }
    if (selected.size === 0) {
      setFormError("Pick at least one peer.");
      return;
    }
    setCreating(true);
    setFormError(null);
    try {
      await onCreate(trimmed, [...selected]);
      setName("");
      setSelected(new Set());
      setShowCreate(false);
    } catch (err) {
      setFormError(err instanceof Error ? err.message : String(err));
    } finally {
      setCreating(false);
    }
  }

  async function handleSend(listName: string) {
    const text = (texts[listName] ?? "").trim();
    if (!text) {
      return;
    }
    setSending((prev) => new Set(prev).add(listName));
    setPanelError(null);
    try {
      const response = await onSend(listName, text);
      setResults((prev) => ({ ...prev, [listName]: response }));
      if (response.delivered + response.queued > 0) {
        setTexts((prev) => ({ ...prev, [listName]: "" }));
      }
    } catch (err) {
      setPanelError(err instanceof Error ? err.message : String(err));
    } finally {
      setSending((prev) => {
        const next = new Set(prev);
        next.delete(listName);
        return next;
      });
    }
  }

  async function handleDelete(listName: string) {
    setPanelError(null);
    try {
      await onDelete(listName);
      setConfirmDelete(null);
      setResults((prev) => {
        const next = { ...prev };
        delete next[listName];
        return next;
      });
    } catch (err) {
      setPanelError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleRename(listName: string) {
    const trimmed = renameName.trim();
    if (!trimmed || trimmed === listName) {
      setRenaming(null);
      return;
    }
    if (sending.has(listName)) {
      // The in-flight send reports its result under the old name; renaming
      // now would let the draft be sent again under the new name.
      setPanelError("Wait for the send to finish before renaming this list.");
      return;
    }
    setPanelError(null);
    try {
      const storedName = (await onRename(listName, trimmed)) || trimmed;
      setRenaming(null);
      // Draft text and send results are keyed by list name; carry
      // them over to the stored name so a rename does not swallow a
      // half-typed message. When the server stores the same name (the
      // requested name normalises back to it), the draft already sits
      // under that key — re-keying would delete it.
      if (storedName !== listName) {
        setTexts((prev) => {
          if (!(listName in prev)) return prev;
          const next = { ...prev, [storedName]: prev[listName] };
          delete next[listName];
          return next;
        });
        setResults((prev) => {
          if (!(listName in prev)) return prev;
          const next = { ...prev, [storedName]: prev[listName] };
          delete next[listName];
          return next;
        });
      }
    } catch (err) {
      setPanelError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <section
      aria-label="Broadcasts"
      className="border-b border-zinc-800 px-3 py-2"
    >
      <div className="flex items-center justify-between">
        <h2 className="text-xs font-bold uppercase tracking-wide text-zinc-500">
          Broadcasts {count > 0 ? `(${count})` : ""}
        </h2>
        <button
          type="button"
          onClick={() => setShowCreate((v) => !v)}
          className="text-xs font-semibold text-cyan-400 hover:text-cyan-300"
        >
          {showCreate ? "Cancel" : "New list"}
        </button>
      </div>

      {loading && <p className="py-2 text-sm text-zinc-500">Loading broadcasts…</p>}
      {error && (
        <p role="alert" className="py-1 text-sm text-red-400">
          Broadcasts unavailable: {error}
        </p>
      )}
      {panelError && (
        <p role="alert" className="py-1 text-sm text-red-400">
          {panelError}
        </p>
      )}

      {showCreate && (
        <div className="mt-2 flex flex-col gap-2 rounded-lg bg-zinc-900 px-2 py-2">
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="List name"
            aria-label="Broadcast list name"
            className="rounded border border-zinc-700 bg-zinc-950 px-2 py-1 text-sm text-zinc-100 placeholder:text-zinc-500"
          />
          {peers.length === 0 ? (
            <p className="text-xs text-zinc-500">No known peers yet.</p>
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
            className="rounded bg-cyan-600 px-3 py-1 text-sm font-semibold text-white hover:bg-cyan-500 disabled:opacity-50"
          >
            {creating ? "Creating…" : "Create list"}
          </button>
        </div>
      )}

      {!loading && !error && count === 0 && !showCreate && (
        <p className="py-1 text-sm text-zinc-500">
          No broadcast lists — create one to message several peers at once.
        </p>
      )}

      {broadcasts.length > 0 && (
        <ul className="mt-1 flex flex-col gap-2">
          {broadcasts.map((list) => {
            const result = results[list.name];
            const busy = sending.has(list.name);
            const hasActiveBeacon = beacons.some(
              (b) =>
                b.active &&
                b.target_kind === "broadcast" &&
                b.target === list.name,
            );
            const showBeacon = hasActiveBeacon || sharing === list.name;
            return (
              <li
                key={list.name}
                className="rounded-lg bg-zinc-900 px-2 py-1.5 text-sm"
              >
                <div className="flex items-center justify-between gap-2">
                  {renaming === list.name ? (
                    <span className="flex min-w-0 flex-1 items-center gap-2">
                      <input
                        type="text"
                        value={renameName}
                        onChange={(e) => setRenameName(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") {
                            e.preventDefault();
                            void handleRename(list.name);
                          }
                          if (e.key === "Escape") {
                            setRenaming(null);
                          }
                        }}
                        placeholder="New list name"
                        aria-label={`Rename ${list.name}`}
                        className="min-w-0 flex-1 rounded border border-zinc-700 bg-zinc-950 px-2 py-1 text-sm text-zinc-100 placeholder:text-zinc-500"
                      />
                      <button
                        type="button"
                        onClick={() => void handleRename(list.name)}
                        className="shrink-0 text-xs font-semibold text-cyan-400 hover:text-cyan-300"
                      >
                        Save
                      </button>
                      <button
                        type="button"
                        onClick={() => setRenaming(null)}
                        className="shrink-0 text-xs text-zinc-400 hover:text-zinc-200"
                      >
                        Cancel
                      </button>
                    </span>
                  ) : (
                    <>
                      <span className="min-w-0 flex-1 truncate font-medium text-zinc-200">
                        {list.name} ({list.members.length})
                      </span>
                      {confirmDelete === list.name ? (
                        <span className="flex shrink-0 gap-2">
                          <button
                            type="button"
                            onClick={() => void handleDelete(list.name)}
                            className="text-xs font-semibold text-red-400 hover:text-red-300"
                          >
                            Confirm delete
                          </button>
                          <button
                            type="button"
                            onClick={() => setConfirmDelete(null)}
                            className="text-xs text-zinc-400 hover:text-zinc-200"
                          >
                            Keep
                          </button>
                        </span>
                      ) : (
                        <span className="flex shrink-0 gap-2">
                          <button
                            type="button"
                            onClick={() => {
                              setRenaming(list.name);
                              setRenameName(list.name);
                              setConfirmDelete(null);
                            }}
                            className="text-xs text-zinc-500 hover:text-zinc-200"
                          >
                            Rename
                          </button>
                          <button
                            type="button"
                            onClick={() => setConfirmDelete(list.name)}
                            className="text-xs text-zinc-500 hover:text-red-400"
                          >
                            Delete
                          </button>
                        </span>
                      )}
                    </>
                  )}
                </div>
                <p className="truncate text-xs text-zinc-400">
                  {list.members.map((m) => m.name).join(", ")}
                </p>
                <textarea
                  value={texts[list.name] ?? ""}
                  onChange={(e) =>
                    setTexts((prev) => ({ ...prev, [list.name]: e.target.value }))
                  }
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      void handleSend(list.name);
                    }
                  }}
                  placeholder={`Message ${list.name}…`}
                  aria-label={`Message ${list.name}`}
                  rows={2}
                  className="mt-1 w-full rounded border border-zinc-700 bg-zinc-950 px-2 py-1 text-sm text-zinc-100 placeholder:text-zinc-500"
                />
                <button
                  type="button"
                  disabled={busy || !(texts[list.name] ?? "").trim()}
                  onClick={() => void handleSend(list.name)}
                  className="mt-1 rounded bg-cyan-600 px-3 py-1 text-xs font-semibold text-white hover:bg-cyan-500 disabled:opacity-50"
                >
                  {busy ? "Sending…" : `Send to ${list.members.length}`}
                </button>
                {result && (
                  <ul className="mt-1 flex flex-col gap-0.5">
                    {result.results.map((member) => (
                      <li key={member.hash} className="text-xs text-zinc-400">
                        {member.name}: {member.status}
                        {member.error ? ` — ${member.error}` : ""}
                      </li>
                    ))}
                  </ul>
                )}
                {showBeacon ? (
                  <div className="mt-1">
                    <BeaconControl
                      target={{
                        kind: "broadcast",
                        key: list.name,
                        name: list.name,
                      }}
                      beacons={beacons}
                      defaultOpen
                      onRefresh={() => void onRefreshBeacons?.()}
                      onError={(message) =>
                        onBeaconError
                          ? onBeaconError(message)
                          : setPanelError(message)
                      }
                    />
                  </div>
                ) : (
                  <button
                    type="button"
                    onClick={() => setSharing(list.name)}
                    className="mt-1 rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-200 hover:bg-zinc-800"
                  >
                    📍 Share live location
                  </button>
                )}
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
