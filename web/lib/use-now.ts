// use-now.ts — a clock tick for relative timestamps.
//
// Provides `now` (unix seconds, updating every `intervalMs`) so components
// rendering "5m ago" / countdowns re-render without polling the node.
// Default tick is 30s; pass 1000 for a live countdown.

"use client";

import { useEffect, useState } from "react";

export function useNow(intervalMs = 30_000): number {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() / 1000), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}
