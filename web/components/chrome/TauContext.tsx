// TauContext.tsx — one shared live node snapshot for the whole shell.
//
// The Shell (layout) provides this; page.tsx and the sidebar consume it,
// so the app opens exactly one /ws connection instead of one per component.
// Tests can supply a fake value through TauContext.Provider directly.

"use client";

import { createContext, useContext } from "react";
import { useTau, type UseTauResult } from "@/lib/use-tau";

export const TauContext = createContext<UseTauResult | null>(null);

export function useTauContext(): UseTauResult {
  const value = useContext(TauContext);
  if (!value) {
    throw new Error("useTauContext must be used inside <TauProvider>");
  }
  return value;
}

export function TauProvider({ children }: { children: React.ReactNode }) {
  const tau = useTau();
  return <TauContext.Provider value={tau}>{children}</TauContext.Provider>;
}
