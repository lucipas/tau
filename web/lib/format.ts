// format.ts — time / bytes / address formatting helpers for the console.
//
// All timestamps from the bridge are unix seconds (floats). All of these
// helpers are pure and safe to call with null/undefined input.

/** Short destination hash: first 8 hex chars, e.g. "a1b2c3d4". */
export function shortHash(hash: string | null | undefined, length = 8): string {
  if (!hash) return "";
  return hash.slice(0, length);
}

/** Display name for a peer: alias > name > short hash. */
export function displayName(
  peer: { alias?: string | null; name?: string | null; destination?: string | null } | null | undefined,
): string {
  if (!peer) return "unknown";
  if (peer.alias) return peer.alias;
  if (peer.name && peer.name !== "unknown") return peer.name;
  return shortHash(peer.destination);
}

/** "a1b2c3d4…" style truncated hash for tight UI spots. */
export function truncatedHash(hash: string | null | undefined): string {
  if (!hash) return "";
  return hash.length > 12 ? `${hash.slice(0, 8)}…` : hash;
}

function pad(n: number): string {
  return String(n).padStart(2, "0");
}

/** "14:32" local time for a unix-seconds timestamp. */
export function formatTime(ts: number | null | undefined): string {
  if (ts == null || !Number.isFinite(ts)) return "";
  const d = new Date(ts * 1000);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/** "Nov 17" local date for a unix-seconds timestamp. */
export function formatDate(ts: number | null | undefined): string {
  if (ts == null || !Number.isFinite(ts)) return "";
  return new Date(ts * 1000).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

/** "Nov 17, 14:32" — full local date + time. */
export function formatDateTime(ts: number | null | undefined): string {
  if (ts == null || !Number.isFinite(ts)) return "";
  return `${formatDate(ts)}, ${formatTime(ts)}`;
}

/**
 * Relative time against `now` (unix seconds; defaults to Date.now()).
 * "just now" < 60s, "5m" < 1h, "3h" < 24h, "2d" < 7d, otherwise a date.
 * Future timestamps clamp to "just now".
 */
export function formatRelative(
  ts: number | null | undefined,
  now: number = Date.now() / 1000,
): string {
  if (ts == null || !Number.isFinite(ts)) return "";
  const diff = Math.max(0, now - ts);
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
  if (diff < 7 * 86400) return `${Math.floor(diff / 86400)}d`;
  return formatDate(ts);
}

/** "in 5m" / "in 2h" / "in 3d" for a future unix-seconds timestamp. */
export function formatCountdown(
  ts: number | null | undefined,
  now: number = Date.now() / 1000,
): string {
  if (ts == null || !Number.isFinite(ts)) return "";
  const diff = ts - now;
  if (diff <= 0) return "now";
  if (diff < 60) return `in ${Math.floor(diff)}s`;
  if (diff < 3600) return `in ${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `in ${Math.floor(diff / 3600)}h`;
  return `in ${Math.floor(diff / 86400)}d`;
}

/** "1.5 KiB" / "10 MiB" / "823 B" — binary units, one decimal. */
export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null || !Number.isFinite(bytes)) return "";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(1)} ${units[unit]}`;
}

/** "1.2k" / "3.4M" compact count for likes, followers, etc. */
export function formatCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "0";
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1).replace(/\.0$/, "")}k`;
  return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
}

/** "5s" / "2m" / "1h" / "3d" for a duration in seconds. */
export function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return "";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

/** "2m ago" style age for a peer's last-seen, from the bridge's `age` field. */
export function formatAge(ageSeconds: number | null | undefined): string {
  if (ageSeconds == null || !Number.isFinite(ageSeconds)) return "";
  return `${formatDuration(ageSeconds)} ago`;
}

/** Expiry label for a disappearing message / post: "expires in 2h". */
export function formatExpiry(
  expiresAt: number | null | undefined,
  now: number = Date.now() / 1000,
): string {
  if (expiresAt == null || !Number.isFinite(expiresAt)) return "";
  if (expiresAt <= now) return "expired";
  return `expires ${formatCountdown(expiresAt, now)}`;
}

/** Extract #tags from post/message text, lowercased, deduped, in order. */
export function extractTags(text: string | null | undefined): string[] {
  if (!text) return [];
  const seen = new Set<string>();
  for (const match of text.matchAll(/#([A-Za-z0-9_]+)/g)) {
    seen.add(match[1].toLowerCase());
  }
  return [...seen];
}

/** Extract @mentions from post/message text, in order. */
export function extractMentions(text: string | null | undefined): string[] {
  if (!text) return [];
  return [...text.matchAll(/@([A-Za-z0-9_]+)/g)].map((m) => m[1]);
}
