import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import PeerPanel from "./PeerPanel";
import { TauContext } from "../chrome/TauContext";
import { fakeTau, makePeer } from "../test-utils";
import type { Conversation } from "@/lib/use-tau";
import type { Peer } from "@/lib/types";

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

function conv(peer: Peer): Conversation {
  return {
    kind: "dm",
    key: PEER_HASH,
    name: "Alice",
    peer,
    lastMessage: null,
    unread: 0,
    muted: false,
    archived: false,
  };
}

function renderPanel(peer: Peer) {
  render(
    <TauContext.Provider value={fakeTau()}>
      <PeerPanel conversation={conv(peer)} />
    </TauContext.Provider>,
  );
}

describe("PeerPanel public profile", () => {
  it("renders the peer's display name, bio and links from the served profile", () => {
    renderPanel(
      makePeer({
        destination: PEER_HASH,
        profile: {
          author: PEER_HASH,
          name: "Alice Liddell",
          bio: "Mesh tinkerer",
          avatar: "",
          links: ["https://example.com"],
          version: 1,
          created_at: 1700000000,
        },
      }),
    );
    expect(screen.getByText("Public profile")).toBeTruthy();
    expect(screen.getByText("Alice Liddell")).toBeTruthy();
    expect(screen.getByText("Mesh tinkerer")).toBeTruthy();
    const link = screen.getByRole("link", { name: "https://example.com" });
    expect(link.getAttribute("href")).toBe("https://example.com");
    expect(link.getAttribute("target")).toBe("_blank");
  });

  it("says so when the peer shared no public profile", () => {
    renderPanel(makePeer({ destination: PEER_HASH, profile: null }));
    expect(screen.getByText("Public profile")).toBeTruthy();
    expect(screen.getByText("This peer hasn't shared a public profile.")).toBeTruthy();
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("renders a profile with an empty bio and no links", () => {
    renderPanel(
      makePeer({
        destination: PEER_HASH,
        profile: {
          author: PEER_HASH,
          name: "Alice Liddell",
          bio: "",
          avatar: "",
          links: [],
          version: 1,
          created_at: 1700000000,
        },
      }),
    );
    expect(screen.getByText("Alice Liddell")).toBeTruthy();
    expect(screen.queryByRole("link")).toBeNull();
  });
});
