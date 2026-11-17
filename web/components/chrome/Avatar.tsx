// Avatar.tsx — shared avatar: colored initials circle, deterministic color
// derived from the peer hash / name. The next agent reuses this for Social,
// Channels, Tools, Settings and Pages.

"use client";

const PALETTE = [
  "bg-cyan-700",
  "bg-teal-700",
  "bg-emerald-700",
  "bg-sky-700",
  "bg-indigo-700",
  "bg-violet-700",
  "bg-fuchsia-700",
  "bg-rose-700",
  "bg-amber-700",
  "bg-lime-700",
];

export function avatarColorIndex(seed: string): number {
  let hash = 0;
  for (let i = 0; i < seed.length; i++) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
  }
  return hash % PALETTE.length;
}

export function initialsFor(name: string | null | undefined, fallbackSeed = ""): string {
  const clean = (name ?? "").trim();
  if (clean && clean.toLowerCase() !== "unknown") {
    const words = clean.split(/\s+/).filter(Boolean);
    const first = words[0]?.[0] ?? "";
    const second = words.length > 1 ? words[1]?.[0] ?? "" : words[0]?.[1] ?? "";
    return (first + second).toUpperCase();
  }
  return (fallbackSeed.slice(0, 2) || "??").toUpperCase();
}

export interface AvatarProps {
  name?: string | null;
  seed?: string | null;
  size?: "sm" | "md" | "lg" | "xl";
  className?: string;
}

const SIZES: Record<NonNullable<AvatarProps["size"]>, string> = {
  sm: "h-7 w-7 text-[11px]",
  md: "h-10 w-10 text-sm",
  lg: "h-12 w-12 text-base",
  xl: "h-20 w-20 text-2xl",
};

export default function Avatar({ name, seed, size = "md", className = "" }: AvatarProps) {
  const key = seed ?? name ?? "";
  const color = PALETTE[avatarColorIndex(key)];
  return (
    <span
      aria-hidden="true"
      className={`inline-flex shrink-0 items-center justify-center rounded-full font-semibold text-zinc-100 ${color} ${SIZES[size]} ${className}`}
    >
      {initialsFor(name, key)}
    </span>
  );
}
