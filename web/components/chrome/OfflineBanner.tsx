// OfflineBanner.tsx — shown when the bridge is unreachable (transport
// "down"): no snapshot and no live socket. Offers a manual retry.

"use client";

import type { UseTauResult } from "@/lib/use-tau";

export default function OfflineBanner({ tau }: { tau: Pick<UseTauResult, "transport" | "connected" | "refresh"> }) {
  if (tau.transport !== "down") return null;
  return (
    <div
      role="alert"
      className="flex items-center justify-center gap-3 border-b border-red-900/60 bg-red-950/80 px-4 py-2 text-sm text-red-200"
    >
      <span aria-hidden="true">●</span>
      <span>
        Bridge unreachable — {tau.connected ? "showing last known state" : "no connection to your tau node"}. Reconnecting…
      </span>
      <button
        type="button"
        onClick={() => void tau.refresh()}
        className="rounded-full border border-red-700 px-3 py-0.5 text-xs font-semibold hover:bg-red-900"
      >
        Retry now
      </button>
    </div>
  );
}
