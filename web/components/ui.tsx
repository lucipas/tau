// Small shared primitives for the rewritten console. Everything is
// plain Tailwind: no fixed heights, no absolute positioning that can
// cover content, rows wrap instead of clipping.
"use client";

import type { ReactNode } from "react";

export function Card({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <section
      className={`min-w-0 rounded-xl border border-line bg-panel ${className}`}
    >
      {children}
    </section>
  );
}

export function CardTitle({
  children,
  right,
}: {
  children: ReactNode;
  right?: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-wrap items-center justify-between gap-2 border-b border-line-soft px-3 py-2">
      <h2 className="text-xs font-semibold tracking-widest text-mist uppercase">
        {children}
      </h2>
      {right}
    </div>
  );
}

export function Button({
  children,
  onClick,
  disabled,
  variant = "ghost",
  title,
  type = "button",
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  variant?: "ghost" | "primary" | "danger" | "subtle";
  title?: string;
  type?: "button" | "submit";
}) {
  const base =
    "inline-flex min-h-9 shrink-0 items-center justify-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors disabled:opacity-50";
  const styles = {
    ghost:
      "border border-line bg-transparent text-ink hover:border-signal-dim hover:text-signal",
    primary: "bg-signal font-semibold text-void hover:bg-signal-dim",
    danger:
      "border border-danger/50 bg-transparent text-danger hover:bg-danger/10",
    subtle: "bg-transparent text-mist hover:bg-panel-2 hover:text-ink",
  } as const;
  return (
    <button
      type={type}
      className={`${base} ${styles[variant]}`}
      onClick={onClick}
      disabled={disabled}
      title={title}
    >
      {children}
    </button>
  );
}

export function Chip({ children }: { children: ReactNode }) {
  return (
    <span className="inline-flex shrink-0 items-center gap-1 rounded-full border border-line bg-panel-2 px-2 py-0.5 text-xs text-mist">
      {children}
    </span>
  );
}

export function EmptyState({ children }: { children: ReactNode }) {
  return <p className="px-3 py-6 text-center text-sm text-faint">{children}</p>;
}

export function ErrorNote({ message }: { message: string | null }) {
  if (!message) return null;
  return (
    <p className="rounded-lg border border-danger/40 bg-danger/10 px-3 py-2 text-sm text-danger">
      {message}
    </p>
  );
}

export function Toggle({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  label: string;
}) {
  return (
    <label className="flex min-w-0 cursor-pointer items-center justify-between gap-3 py-2">
      <span className="min-w-0 text-sm">{label}</span>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={label}
        onClick={() => onChange(!checked)}
        className={`h-6 w-11 shrink-0 rounded-full border border-line transition-colors ${
          checked ? "bg-signal" : "bg-panel-2"
        }`}
      >
        <span
          className={`block h-5 w-5 rounded-full bg-ink transition-transform ${
            checked ? "translate-x-5" : "translate-x-0.5"
          }`}
        />
      </button>
    </label>
  );
}
