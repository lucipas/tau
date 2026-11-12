"use client";

import { useEffect, useRef, useState } from "react";

type IconProps = {
  className?: string;
};

export function SignalIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      className={className}
      aria-hidden="true"
    >
      <path d="M4.5 15.5a10 10 0 0 1 15 0" />
      <path d="M7.5 18a6 6 0 0 1 9 0" />
      <circle cx="12" cy="20" r="1" fill="currentColor" stroke="none" />
      <path d="M2 12.5a14 14 0 0 1 20 0" opacity=".45" />
    </svg>
  );
}

export function SendIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <path d="m4 4 16 8-16 8 3.2-8L4 4Z" />
      <path d="M7.2 12H20" />
    </svg>
  );
}

export function CopyIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <rect x="9" y="9" width="11" height="11" rx="2" />
      <path d="M5 15H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v1" />
    </svg>
  );
}

export function CheckIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <path d="m5 13 4 4L19 7" />
    </svg>
  );
}

export function RadarIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      className={className}
      aria-hidden="true"
    >
      <circle cx="12" cy="12" r="9" opacity=".35" />
      <circle cx="12" cy="12" r="5" opacity=".55" />
      <circle cx="12" cy="12" r="1.4" fill="currentColor" stroke="none" />
      <path d="M12 12 18.4 5.6" />
      <path d="M12 3a9 9 0 0 1 9 9" opacity=".8" />
    </svg>
  );
}

export function BoltIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <path d="M13 2 4.5 13.5H11l-1 8.5 8.5-11.5H12l1-8.5Z" />
    </svg>
  );
}

export function PingIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <path d="M3 12h4l2.5-6 4 12 2.5-6h5" />
    </svg>
  );
}

export function InfoIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <circle cx="12" cy="12" r="9" />
      <path d="M12 11v5" />
      <path d="M12 8h.01" />
    </svg>
  );
}

export function DownloadIcon({ className }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <path d="M12 3v12" />
      <path d="m6.5 10 5.5 5.5L17.5 10" />
      <path d="M4 20h16" />
    </svg>
  );
}

export function CopyButton({
  value,
  label = "Copy",
  className = "",
}: {
  value: string;
  label?: string;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timer.current) clearTimeout(timer.current);
    };
  }, []);

  async function copy() {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      if (timer.current) clearTimeout(timer.current);
      timer.current = setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  }

  return (
    <button
      type="button"
      onClick={copy}
      title={copied ? "Copied" : `${label}: ${value}`}
      className={`inline-flex items-center gap-1.5 rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 ${className}`}
    >
      {copied ? (
        <CheckIcon className="h-3.5 w-3.5" />
      ) : (
        <CopyIcon className="h-3.5 w-3.5" />
      )}
      <span>{copied ? "copied" : label}</span>
    </button>
  );
}

export function StatusDot({
  state,
}: {
  state: "live" | "connecting" | "reconnecting";
}) {
  const palette =
    state === "live"
      ? "bg-signal shadow-[0_0_10px_rgba(45,212,191,0.8)]"
      : state === "connecting"
        ? "bg-amber"
        : "bg-rose-400";
  return (
    <span className="relative flex h-2 w-2" aria-hidden="true">
      <span
        className={`absolute inline-flex h-full w-full rounded-full opacity-60 ${
          state === "live" ? "animate-ping" : ""
        } ${palette}`}
      />
      <span className={`relative inline-flex h-2 w-2 rounded-full ${palette}`} />
    </span>
  );
}

export function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <span className="font-mono text-[10px] uppercase tracking-[0.22em] text-mist">
      {children}
    </span>
  );
}
