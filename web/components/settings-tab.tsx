"use client";

import { useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "@/lib/api";
import type { UseTauResult } from "@/lib/use-tau";
import { peerLabel, shortHash } from "@/lib/format";
import type {
  TanRules,
  TanRulesResponse,
  TanRulesUpdateResponse,
} from "@/lib/types";
import { Button, Card, CardTitle, EmptyState, ErrorNote, Toggle } from "./ui";

const inputClass =
  "min-w-0 flex-1 rounded-lg border border-line bg-panel-2 px-3 py-2 text-sm text-ink placeholder:text-faint focus:border-signal-dim focus:outline-none";

export function SettingsTab({ tau }: { tau: UseTauResult }) {
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <NodeSettingsCard tau={tau} />
      <RenameCard tau={tau} />
      <DisappearingCard tau={tau} />
      <MutedCard tau={tau} />
      <ArchivedCard tau={tau} />
      <SocialRulesCard />
    </div>
  );
}

function NodeSettingsCard({ tau }: { tau: UseTauResult }) {
  const [pendingKey, setPendingKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function toggle(key: "accept_files" | "send_read_receipts") {
    if (tau.settings === null || pendingKey !== null) return;
    setPendingKey(key);
    setError(null);
    try {
      await tau.updateSettings({ [key]: !tau.settings[key] });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "setting failed");
    } finally {
      setPendingKey(null);
    }
  }

  return (
    <Card>
      <CardTitle>Node settings</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Changes apply to the running node at once — no restart.
        </p>
        <ErrorNote message={error} />
        {tau.settings === null ? (
          <p className="text-sm text-faint">Loading settings…</p>
        ) : (
          <>
            <Toggle
              checked={tau.settings.accept_files}
              onChange={() => void toggle("accept_files")}
              label="Accept file transfers"
            />
            <p className="-mt-2 text-sm leading-relaxed text-mist">
              Off by default — an unsolicited file is never written to disk
              unless you turn this on.
            </p>
            <Toggle
              checked={tau.settings.send_read_receipts}
              onChange={() => void toggle("send_read_receipts")}
              label="Send read receipts"
            />
            <p className="-mt-2 text-sm leading-relaxed text-mist">
              On by default — peers see when you have read their messages.
              Turning it off sends nothing.
            </p>
            {pendingKey !== null ? (
              <p className="text-sm text-faint">Saving…</p>
            ) : null}
          </>
        )}
      </div>
    </Card>
  );
}

function RenameCard({ tau }: { tau: UseTauResult }) {
  const [name, setName] = useState(tau.node?.name ?? "");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  // Keep the field in step when the node's name arrives or changes
  // elsewhere (another console, a rename from the CLI).
  useEffect(() => {
    setName(tau.node?.name ?? "");
  }, [tau.node?.name]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const next = name.trim();
    if (next === "" || saving) return;
    setSaving(true);
    setError(null);
    setNotice(null);
    try {
      await tau.renameNode(next);
      setNotice(`Node renamed to ${next}.`);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "rename failed");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card>
      <CardTitle>Node name</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          The name this node announces to the mesh.
        </p>
        <ErrorNote message={error} />
        {notice ? <p className="text-sm text-signal">{notice}</p> : null}
        <form onSubmit={submit} className="flex min-w-0 gap-2">
          <label htmlFor="node-name" className="sr-only">
            Node name
          </label>
          <input
            id="node-name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="node name"
            autoComplete="off"
            className={inputClass}
          />
          <Button
            type="submit"
            variant="primary"
            disabled={name.trim() === "" || saving}
          >
            {saving ? "renaming…" : "rename"}
          </Button>
        </form>
      </div>
    </Card>
  );
}

function peerName(tau: UseTauResult, destination: string): string {
  const peer = tau.peers.find((entry) => entry.destination === destination);
  return peer ? peerLabel(peer.name, destination) : shortHash(destination);
}

function groupName(tau: UseTauResult, id: string): string {
  const group = tau.groups.find((entry) => entry.id === id);
  return group ? group.name : shortHash(id);
}

function formatLifetime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

function DisappearingCard({ tau }: { tau: UseTauResult }) {
  const [target, setTarget] = useState("");
  const [seconds, setSeconds] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const defaults = tau.disappearingDefaults;
  const peerEntries = Object.entries(defaults.peers);
  const groupEntries = Object.entries(defaults.groups);

  function parseTarget(
    value: string,
  ): { target: string; group: boolean } | null {
    if (value.startsWith("peer:")) {
      return { target: value.slice("peer:".length), group: false };
    }
    if (value.startsWith("group:")) {
      return { target: value.slice("group:".length), group: true };
    }
    return null;
  }

  async function setDefault(
    value: string,
    secondsValue: number | null,
  ): Promise<boolean> {
    const parsed = parseTarget(value);
    if (!parsed) return false;
    setSaving(true);
    setError(null);
    setNotice(null);
    try {
      await tau.setDisappearing(parsed.target, parsed.group, secondsValue);
      setNotice(
        secondsValue === null
          ? "Disappearing messages turned off for that conversation."
          : `Messages disappear after ${formatLifetime(secondsValue)}.`,
      );
      return true;
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "could not save default",
      );
      return false;
    } finally {
      setSaving(false);
    }
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const trimmed = seconds.trim();
    // Blank means off: null seconds clears the default.
    const parsedSeconds =
      trimmed === "" ? null : Number.parseInt(trimmed, 10);
    if (parsedSeconds !== null && (!Number.isFinite(parsedSeconds) || parsedSeconds <= 0)) {
      setError("Enter a positive number of seconds, or leave it blank to turn off.");
      return;
    }
    if (await setDefault(target, parsedSeconds)) {
      setSeconds("");
    }
  }

  return (
    <Card>
      <CardTitle>Disappearing messages</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Default lifetimes for new messages in a conversation. Blank
          seconds turns the default off.
        </p>
        <ErrorNote message={error} />
        {notice ? <p className="text-sm text-signal">{notice}</p> : null}
        {peerEntries.length === 0 && groupEntries.length === 0 ? (
          <p className="text-sm text-faint">
            No disappearing-message defaults set.
          </p>
        ) : (
          <ul className="flex flex-col gap-2">
            {peerEntries.map(([destination, lifetime]) => (
              <li
                key={`peer:${destination}`}
                className="flex min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <span className="min-w-0 break-words text-sm text-ink">
                  {peerName(tau, destination)} · {formatLifetime(lifetime)}
                </span>
                <Button
                  disabled={saving}
                  onClick={() => void setDefault(`peer:${destination}`, null)}
                >
                  turn off
                </Button>
              </li>
            ))}
            {groupEntries.map(([id, lifetime]) => (
              <li
                key={`group:${id}`}
                className="flex min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <span className="min-w-0 break-words text-sm text-ink">
                  {groupName(tau, id)} · {formatLifetime(lifetime)}
                </span>
                <Button
                  disabled={saving}
                  onClick={() => void setDefault(`group:${id}`, null)}
                >
                  turn off
                </Button>
              </li>
            ))}
          </ul>
        )}
        <form onSubmit={submit} className="flex min-w-0 flex-wrap gap-2">
          <label htmlFor="disappearing-target" className="sr-only">
            Conversation
          </label>
          <select
            id="disappearing-target"
            value={target}
            onChange={(event) => setTarget(event.target.value)}
            className="min-h-9 min-w-0 flex-1 rounded-lg border border-line bg-panel-2 px-3 py-2 text-sm text-ink focus:border-signal-dim focus:outline-none"
          >
            <option value="">pick a peer or group…</option>
            {tau.peers.map((peer) => (
              <option key={peer.destination} value={`peer:${peer.destination}`}>
                {peer.name || shortHash(peer.destination)}
              </option>
            ))}
            {tau.groups.map((group) => (
              <option key={group.id} value={`group:${group.id}`}>
                {group.name}
              </option>
            ))}
          </select>
          <label htmlFor="disappearing-seconds" className="sr-only">
            Seconds until messages disappear (blank to turn off)
          </label>
          <input
            id="disappearing-seconds"
            value={seconds}
            onChange={(event) => setSeconds(event.target.value)}
            placeholder="seconds (blank = off)"
            inputMode="numeric"
            autoComplete="off"
            className="min-h-9 w-36 min-w-0 rounded-lg border border-line bg-panel-2 px-3 py-2 text-sm text-ink placeholder:text-faint focus:border-signal-dim focus:outline-none"
          />
          <Button
            type="submit"
            variant="primary"
            disabled={target === "" || saving}
          >
            {saving ? "saving…" : "set default"}
          </Button>
        </form>
      </div>
    </Card>
  );
}

function MutedCard({ tau }: { tau: UseTauResult }) {
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const muted = tau.mutedConversations;

  async function unmute(target: string, group: boolean) {
    if (pending !== null) return;
    setPending(target);
    setError(null);
    try {
      await tau.muteConversation(target, group, false);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "unmute failed");
    } finally {
      setPending(null);
    }
  }

  const empty = muted.peers.length === 0 && muted.groups.length === 0;

  return (
    <Card>
      <CardTitle>Muted conversations</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Muted conversations still list their messages, but never count as
          unread.
        </p>
        <ErrorNote message={error} />
        {empty ? (
          <EmptyState>No conversations muted.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-2">
            {muted.peers.map((destination) => (
              <li
                key={`peer:${destination}`}
                className="flex min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <span className="min-w-0 break-words text-sm text-ink">
                  {peerName(tau, destination)}
                </span>
                <Button
                  disabled={pending === destination}
                  onClick={() => void unmute(destination, false)}
                >
                  {pending === destination ? "unmuting…" : "unmute"}
                </Button>
              </li>
            ))}
            {muted.groups.map((id) => (
              <li
                key={`group:${id}`}
                className="flex min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <span className="min-w-0 break-words text-sm text-ink">
                  {groupName(tau, id)}
                </span>
                <Button
                  disabled={pending === id}
                  onClick={() => void unmute(id, true)}
                >
                  {pending === id ? "unmuting…" : "unmute"}
                </Button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Card>
  );
}

function ArchivedCard({ tau }: { tau: UseTauResult }) {
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const archived = tau.archivedConversations;

  async function unarchive(destination: string) {
    if (pending !== null) return;
    setPending(destination);
    setError(null);
    try {
      await tau.archiveConversation(destination, false);
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "unarchive failed",
      );
    } finally {
      setPending(null);
    }
  }

  return (
    <Card>
      <CardTitle>Archived conversations</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <ErrorNote message={error} />
        {archived.length === 0 ? (
          <EmptyState>No conversations archived.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-2">
            {archived.map((entry) => (
              <li
                key={entry.destination}
                className="flex min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <span className="min-w-0 break-words text-sm text-ink">
                  {entry.name || shortHash(entry.destination)} ·{" "}
                  {entry.messages} message{entry.messages === 1 ? "" : "s"}
                </span>
                <Button
                  disabled={pending === entry.destination}
                  onClick={() => void unarchive(entry.destination)}
                >
                  {pending === entry.destination ? "unarchiving…" : "unarchive"}
                </Button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Card>
  );
}

function SocialRulesCard() {
  const [rules, setRules] = useState<TanRules | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [blockInput, setBlockInput] = useState("");
  const [allowInput, setAllowInput] = useState("");
  // Last rules the server confirmed, so a failed save can revert the
  // draft instead of showing a rule the node is not enforcing.
  const confirmedRef = useRef<TanRules | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const result = await apiGet<TanRulesResponse>("/api/tan/rules");
        if (!cancelled) {
          confirmedRef.current = result.rules;
          setRules(result.rules);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(
            caught instanceof Error ? caught.message : "could not load rules",
          );
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  async function apply(next: TanRules) {
    setRules(next);
    setSaveError(null);
    setSaving(true);
    try {
      const result = await apiPost<TanRulesUpdateResponse>("/api/tan/rules", {
        ...next,
      });
      confirmedRef.current = result.rules;
      setRules(result.rules);
    } catch (caught) {
      // Revert to the last server-confirmed rules so the section never
      // shows a rule the node is not enforcing.
      setRules(confirmedRef.current);
      setSaveError(
        caught instanceof Error ? caught.message : "could not save rules",
      );
    } finally {
      setSaving(false);
    }
  }

  function addAuthor(
    list: "allow_authors" | "block_authors",
    value: string,
  ) {
    if (!rules) return;
    const target = value.trim();
    if (!target) return;
    if (
      rules[list].some((entry) => entry.toLowerCase() === target.toLowerCase())
    ) {
      return;
    }
    void apply({ ...rules, [list]: [...rules[list], target] });
    if (list === "allow_authors") setAllowInput("");
    else setBlockInput("");
  }

  function removeAuthor(
    list: "allow_authors" | "block_authors",
    value: string,
  ) {
    if (!rules) return;
    void apply({
      ...rules,
      [list]: rules[list].filter((entry) => entry !== value),
    });
  }

  return (
    <Card>
      <CardTitle>Social rules</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Which remote authors&apos; Tan posts this node stores. Your own
          posts are never filtered.
        </p>
        {loading && !rules ? (
          <p className="text-sm text-faint">Loading rules…</p>
        ) : !rules ? (
          <ErrorNote message={error ?? "could not load rules"} />
        ) : (
          <>
            <div className="flex min-w-0 flex-col gap-2">
              <p className="text-xs font-semibold tracking-widest text-mist uppercase">
                Default
              </p>
              <div className="flex min-w-0 gap-1 rounded-lg border border-line bg-panel-2 p-1">
                {(["allow", "deny"] as const).map((policy) => (
                  <button
                    key={policy}
                    type="button"
                    disabled={saving}
                    onClick={() => void apply({ ...rules, default: policy })}
                    className={`min-h-9 min-w-0 flex-1 rounded-md px-2 py-1 text-sm disabled:opacity-50 ${
                      rules.default === policy
                        ? "bg-signal/15 text-signal"
                        : "text-mist"
                    }`}
                  >
                    {policy === "allow" ? "allow all" : "deny all"}
                  </button>
                ))}
              </div>
              <p className="text-sm leading-relaxed text-mist">
                {rules.default === "allow"
                  ? "Every well-signed post is stored unless its author is blocked."
                  : "Only allow-listed authors — and followed authors, if enabled — are stored."}
              </p>
            </div>
            <Toggle
              checked={rules.allow_followed}
              onChange={(next) =>
                void apply({ ...rules, allow_followed: next })
              }
              label="Admit authors you follow"
            />
            <AuthorList
              label="Block list"
              hint="Never admit these authors."
              authors={rules.block_authors}
              input={blockInput}
              onInput={setBlockInput}
              onAdd={(value) => addAuthor("block_authors", value)}
              onRemove={(value) => removeAuthor("block_authors", value)}
              disabled={saving}
            />
            <AuthorList
              label="Allow list"
              hint="Always admit these authors."
              authors={rules.allow_authors}
              input={allowInput}
              onInput={setAllowInput}
              onAdd={(value) => addAuthor("allow_authors", value)}
              onRemove={(value) => removeAuthor("allow_authors", value)}
              disabled={saving}
            />
            <ErrorNote message={saveError ?? error} />
          </>
        )}
      </div>
    </Card>
  );
}

function AuthorList({
  label,
  hint,
  authors,
  input,
  onInput,
  onAdd,
  onRemove,
  disabled,
}: {
  label: string;
  hint: string;
  authors: string[];
  input: string;
  onInput: (value: string) => void;
  onAdd: (value: string) => void;
  onRemove: (value: string) => void;
  disabled: boolean;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-2">
      <p className="text-xs font-semibold tracking-widest text-mist uppercase">
        {label}
      </p>
      <p className="text-sm leading-relaxed text-mist">{hint}</p>
      <form
        className="flex min-w-0 gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          onAdd(input);
        }}
      >
        <input
          value={input}
          onChange={(event) => onInput(event.target.value)}
          placeholder="name or author hash"
          aria-label={`${label} author`}
          spellCheck={false}
          disabled={disabled}
          className={inputClass}
        />
        <Button type="submit" disabled={disabled || !input.trim()}>
          add
        </Button>
      </form>
      {authors.length === 0 ? null : (
        <ul className="flex flex-col gap-1.5">
          {authors.map((author) => (
            <li
              key={author}
              className="flex min-w-0 items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
            >
              <span
                className="min-w-0 break-all text-sm text-ink"
                title={author}
              >
                {shortHash(author, 10)}
              </span>
              <Button
                disabled={disabled}
                onClick={() => onRemove(author)}
              >
                remove
              </Button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default SettingsTab;
