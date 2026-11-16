"use client";

import { SocialConsole } from "@/components/social-console";
import { useTau } from "@/lib/use-tau";

export default function Social() {
  // The standalone page offers the same share targets as the embedded
  // console, which takes its peers and groups from this same hook.
  const tau = useTau();
  return <SocialConsole peers={tau.peers} groups={tau.groups} />;
}
