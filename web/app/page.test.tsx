import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import Home from "./page";
import { TauContext } from "@/components/chrome/TauContext";
import { fakeTau, makeMessage, makePeer, makeUnread, stubBridgeFetch } from "@/components/test-utils";

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
const peer = makePeer({ destination: PEER_HASH, name: "Alice" });
const tau = fakeTau({
  peers: [peer],
  messages: [makeMessage({ id: 1, text: "hey there", peer: PEER_HASH, peer_name: "Alice" })],
  unread: makeUnread({ per_peer: { [PEER_HASH]: 1 }, total: 1 }),
});

function renderHome() {
  return render(
    <TauContext.Provider value={tau}>
      <Home />
    </TauContext.Provider>,
  );
}

describe("page hash routing", () => {
  beforeEach(() => {
    stubBridgeFetch();
    window.location.hash = "#chat";
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    window.location.hash = "#chat";
  });

  it("renders the chat list on #chat", () => {
    renderHome();
    expect(screen.getByText("Chat")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(screen.getByLabelText("1 unread")).toBeTruthy();
  });

  it("opens the thread when a conversation is clicked", async () => {
    renderHome();
    fireEvent.click(screen.getByText("Alice"));
    expect(window.location.hash).toBe(`#chat?peer=${PEER_HASH}`);
    expect(await screen.findByPlaceholderText("Message Alice…")).toBeTruthy();
    expect(screen.getAllByText("hey there").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByLabelText("Back to conversations")).toBeTruthy();
  });

  it("opens the thread directly from a deep-linked hash", async () => {
    window.location.hash = `#chat?peer=${PEER_HASH}`;
    renderHome();
    expect(await screen.findByPlaceholderText("Message Alice…")).toBeTruthy();
  });

  it("renders the Social placeholder on #social", () => {
    window.location.hash = "#social";
    renderHome();
    expect(screen.getByText("Social")).toBeTruthy();
    expect(screen.getByText(/Tan feed lives here/)).toBeTruthy();
  });

  it("renders the Channels placeholder on #channels and Pages on #pages", () => {
    window.location.hash = "#channels";
    const { unmount } = renderHome();
    expect(screen.getByText("Channels")).toBeTruthy();
    unmount();
    window.location.hash = "#pages";
    renderHome();
    expect(screen.getByText("Pages")).toBeTruthy();
  });

  it("renders the People view on #people", () => {
    window.location.hash = "#people";
    renderHome();
    expect(screen.getByText("People")).toBeTruthy();
    expect(screen.getByText(/1 peer/)).toBeTruthy();
  });

  it("shows the peer panel on #people with a peer selected", async () => {
    window.location.hash = `#people?peer=${PEER_HASH}`;
    renderHome();
    expect(await screen.findByText("Safety number")).toBeTruthy();
    expect(screen.getByText("← Back to chat")).toBeTruthy();
  });
});
