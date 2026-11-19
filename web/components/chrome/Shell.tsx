// Shell.tsx — the app shell: offline banner, left nav rail, center column
// (page content), right sidebar, mobile bottom tab bar. Provides the single
// shared TauContext for the whole tree. Alt+1–6 switches sections.

"use client";

import { useEffect } from "react";
import { TauProvider, useTauContext } from "./TauContext";
import { SECTIONS, useHash, type Section } from "./useHash";
import { useBeaconFeed } from "@/lib/use-tau";
import LeftNav from "./LeftNav";
import RightSidebar from "./RightSidebar";
import MobileTabBar from "./MobileTabBar";
import OfflineBanner from "./OfflineBanner";

function ShellInner({ children }: { children: React.ReactNode }) {
  const tau = useTauContext();
  const { section, navigate } = useHash();
  const unreadTotal = tau.unread?.total ?? 0;
  // Live beacons keep feeding from the browser's geolocation even when
  // the user leaves the chat tab (the feed lives in the shell, not the
  // chat section, for exactly this reason).
  useBeaconFeed();

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!e.altKey || e.metaKey || e.ctrlKey) return;
      const idx = ["1", "2", "3", "4", "5", "6"].indexOf(e.key);
      if (idx >= 0 && SECTIONS[idx]) {
        e.preventDefault();
        navigate(SECTIONS[idx] as Section);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [navigate]);

  return (
    <div className="flex h-dvh flex-col overflow-hidden bg-zinc-950 text-zinc-100">
      <OfflineBanner tau={tau} />
      <div className="mx-auto flex min-h-0 w-full max-w-[1280px] flex-1 justify-center">
        <div className="hidden shrink-0 overflow-y-auto lg:block">
          <LeftNav active={section} unreadTotal={unreadTotal} tau={tau} onNavigate={navigate} />
        </div>
        <main className="min-w-0 flex-1 overflow-y-auto border-x border-zinc-800 pb-16 lg:max-w-[600px] lg:pb-0">
          {children}
        </main>
        <div className="hidden w-[320px] shrink-0 overflow-y-auto lg:block">
          <RightSidebar />
        </div>
      </div>
      <MobileTabBar active={section} unreadTotal={unreadTotal} onNavigate={navigate} />
    </div>
  );
}

export default function Shell({ children }: { children: React.ReactNode }) {
  return (
    <TauProvider>
      <ShellInner>{children}</ShellInner>
    </TauProvider>
  );
}
