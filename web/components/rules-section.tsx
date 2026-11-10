"use client";

import { useEffect, useState } from "react";

import { shortHash } from "@/lib/format";
import type { TanRules } from "@/lib/types";

import { SectionLabel } from "./ui";

interface RulesSectionProps {
  rules: TanRules | null;
  availableKinds: string[];
  loading: boolean;
  error: string | null;
  onUpdate: (next: TanRules) => Promise<TanRules>;
  onRefresh: () => Promise<void>;
}

export function RulesSection({
  rules,
  availableKinds,
  loading,
  error,
  onUpdate,
  onRefresh,
}: RulesSectionProps) {
  // A local draft so a toggle or an add applies the moment it is made,
  // like the rest of the console. A failed save reverts to the last
  // rules the server confirmed, so the section never shows a rule the
  // node is not enforcing.
  const [draft, setDraft] = useState<TanRules | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [allowInput, setAllowInput] = useState("");
  const [blockInput, setBlockInput] = useState("");

  useEffect(() => {
    setDraft(rules);
  }, [rules]);

  async function apply(next: TanRules) {
    setDraft(next);
    setSaveError(null);
    setSaving(true);
    try {
      await onUpdate(next);
    } catch (caught) {
      setDraft(rules);
      setSaveError(
        caught instanceof Error ? caught.message : "could not save rules",
      );
    } finally {
      setSaving(false);
    }
  }

  function toggleKind(kind: string) {
    if (!draft) return;
    const shown = draft.kinds ?? availableKinds;
    const next = shown.includes(kind)
      ? shown.filter((entry) => entry !== kind)
      : [...shown, kind];
    void apply({
      ...draft,
      kinds:
        next.length === availableKinds.length
          ? null
          : [...next].sort((a, b) => a.localeCompare(b)),
    });
  }

  function addAuthor(list: "allow_authors" | "block_authors", value: string) {
    if (!draft) return;
    const target = value.trim();
    if (!target) return;
    if (
      draft[list].some((entry) => entry.toLowerCase() === target.toLowerCase())
    ) {
      return;
    }
    void apply({ ...draft, [list]: [...draft[list], target] });
    if (list === "allow_authors") setAllowInput("");
    else setBlockInput("");
  }

  function removeAuthor(list: "allow_authors" | "block_authors", value: string) {
    if (!draft) return;
    void apply({
      ...draft,
      [list]: draft[list].filter((entry) => entry !== value),
    });
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
      <div className="flex items-center justify-between">
        <SectionLabel>admission rules</SectionLabel>
        <button
          type="button"
          onClick={() => void onRefresh()}
          disabled={loading}
          className="font-mono text-[10px] text-mist/70 transition hover:text-signal disabled:opacity-50"
          title="reload rules from the node"
        >
          refresh
        </button>
      </div>
      <p className="mt-2 text-[11px] leading-relaxed text-mist">
        Which remote authors&apos; posts this node stores. Your own posts
        are never filtered.
      </p>

      {loading && !draft ? (
        <p className="mt-3 text-[11px] text-mist">loading rules…</p>
      ) : !draft ? (
        <p className="mt-3 text-[11px] text-rose-300">
          {error ?? "could not load rules"}
        </p>
      ) : (
        <>
          <div className="mt-3">
            <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-mist">
              default
            </p>
            <div className="mt-1.5 flex items-center gap-1 rounded-lg border border-line/70 bg-void/50 p-1 text-[11px]">
              {(["allow", "deny"] as const).map((policy) => (
                <button
                  key={policy}
                  type="button"
                  disabled={saving}
                  onClick={() => void apply({ ...draft, default: policy })}
                  className={`flex-1 rounded px-2 py-1 ${
                    draft.default === policy
                      ? "bg-signal/15 text-signal"
                      : "text-mist"
                  } disabled:opacity-50`}
                >
                  {policy === "allow" ? "allow all" : "deny all"}
                </button>
              ))}
            </div>
            <p className="mt-1.5 text-[11px] leading-relaxed text-mist/80">
              {draft.default === "allow"
                ? "Every well-signed post is stored unless its author is blocked."
                : "Only allow-listed authors — and followed authors, if enabled — are stored."}
            </p>
          </div>

          <label className="mt-3 flex cursor-pointer items-center gap-2 text-xs text-zinc-200">
            <input
              type="checkbox"
              checked={draft.allow_followed}
              disabled={saving}
              onChange={() =>
                void apply({
                  ...draft,
                  allow_followed: !draft.allow_followed,
                })
              }
              className="h-3.5 w-3.5 accent-teal-400"
            />
            admit authors you follow
          </label>

          <div className="mt-3">
            <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-mist">
              kinds shown
            </p>
            <p className="mt-1 text-[11px] leading-relaxed text-mist/80">
              Display only — every kind from an admitted author is still
              stored, so chains stay complete.
            </p>
            <div className="mt-1.5 flex flex-wrap gap-1.5">
              {availableKinds.map((kind) => {
                const shown = draft.kinds === null || draft.kinds.includes(kind);
                return (
                  <button
                    key={kind}
                    type="button"
                    disabled={saving}
                    onClick={() => toggleKind(kind)}
                    aria-pressed={shown}
                    className={`rounded-lg border px-2 py-1 font-mono text-[11px] transition disabled:opacity-50 ${
                      shown
                        ? "border-signal/40 bg-signal/10 text-signal"
                        : "border-line bg-void/70 text-mist"
                    }`}
                  >
                    {kind}
                  </button>
                );
              })}
            </div>
          </div>

          <AuthorList
            label="allow list"
            hint="always admit these authors"
            authors={draft.allow_authors}
            input={allowInput}
            onInput={setAllowInput}
            onAdd={(value) => addAuthor("allow_authors", value)}
            onRemove={(value) => removeAuthor("allow_authors", value)}
            disabled={saving}
          />
          <AuthorList
            label="block list"
            hint="never admit these authors"
            authors={draft.block_authors}
            input={blockInput}
            onInput={setBlockInput}
            onAdd={(value) => addAuthor("block_authors", value)}
            onRemove={(value) => removeAuthor("block_authors", value)}
            disabled={saving}
          />

          {saveError ?? error ? (
            <p className="mt-3 text-[11px] leading-relaxed text-rose-300">
              {saveError ?? error}
            </p>
          ) : null}
        </>
      )}
    </section>
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
    <div className="mt-3">
      <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-mist">
        {label}
      </p>
      <p className="mt-1 text-[11px] leading-relaxed text-mist/80">{hint}</p>
      <form
        className="mt-1.5 flex gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          onAdd(input);
        }}
      >
        <input
          value={input}
          onChange={(event) => onInput(event.target.value)}
          placeholder="name or author hash"
          spellCheck={false}
          disabled={disabled}
          className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20 disabled:opacity-50"
        />
        <button
          type="submit"
          disabled={disabled || !input.trim()}
          className="rounded-lg border border-line bg-void/70 px-2.5 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
        >
          add
        </button>
      </form>
      {authors.length === 0 ? null : (
        <ul className="mt-2 space-y-1.5">
          {authors.map((author) => (
            <li
              key={author}
              className="flex items-center justify-between gap-2 rounded-lg border border-line/60 bg-void/40 px-2.5 py-2"
            >
              <span
                className="min-w-0 truncate font-mono text-[11px] text-zinc-200"
                title={author}
              >
                {shortHash(author, 10)}
              </span>
              <button
                type="button"
                disabled={disabled}
                onClick={() => onRemove(author)}
                className="shrink-0 font-mono text-[10px] text-mist/70 transition hover:text-rose-300 disabled:opacity-50"
                title="remove"
              >
                remove
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
