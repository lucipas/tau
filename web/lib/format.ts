export function shortHash(hash: string | null | undefined, size = 10): string {
  if (!hash) return "unknown";
  if (hash.length <= size + 2) return hash;
  return `${hash.slice(0, size)}…${hash.slice(-4)}`;
}

export function formatTime(timestamp: number): string {
  if (!timestamp) return "--:--:--";
  return new Date(timestamp * 1000).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export function formatDateTime(timestamp: number): string {
  if (!timestamp) return "unknown";
  return new Date(timestamp * 1000).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export function formatAge(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "unknown";
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${Math.floor(seconds)}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

export function formatUptime(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "0s";
  const total = Math.floor(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  if (minutes > 0) return `${minutes}m ${String(secs).padStart(2, "0")}s`;
  return `${secs}s`;
}

export function formatRtt(rtt: number | null | undefined): string {
  if (rtt === null || rtt === undefined) return "no reply";
  if (rtt < 1) return `${rtt.toFixed(2)} ms`;
  return `${rtt.toFixed(1)} ms`;
}

export function peerLabel(name: string, hash: string): string {
  return `${name || "unknown"} · ${shortHash(hash, 8)}`;
}
