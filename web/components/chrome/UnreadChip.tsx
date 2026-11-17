// UnreadChip.tsx — small unread-count badge. Renders nothing at zero.

"use client";

export default function UnreadChip({ count, className = "" }: { count: number; className?: string }) {
  if (!count || count <= 0) return null;
  return (
    <span
      aria-label={`${count} unread`}
      className={`inline-flex min-w-5 items-center justify-center rounded-full bg-cyan-500 px-1.5 py-0.5 text-[11px] font-bold leading-none text-zinc-950 ${className}`}
    >
      {count > 99 ? "99+" : count}
    </span>
  );
}
