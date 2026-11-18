import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import LeftNav from "./LeftNav";
import { fakeTau } from "../test-utils";

describe("LeftNav", () => {
  it("marks the active section and shows the unread chip on Chat", () => {
    const tau = fakeTau({ transport: "ws", node: { name: "node", destination: "aa", protocol: 1, version: "1", rns_version: "1", uptime: 1, peers: 2, mailbox: { outbox: 0, held: 0, seen: 0, sent: 0, receipts: 0, retracted: 0, sent_retracted: 0 }, interfaces: [], lxmf: { enabled: false, delivery: null, propagation: null } } });
    render(<LeftNav active="social" unreadTotal={3} tau={tau} onNavigate={() => undefined} />);
    const social = screen.getByText("Social").closest("a");
    expect(social?.getAttribute("aria-current")).toBe("page");
    const chat = screen.getByText("Chat").closest("a");
    expect(chat?.getAttribute("aria-current")).toBeNull();
    expect(screen.getByLabelText("3 unread")).toBeTruthy();
    expect(screen.getByText("Live")).toBeTruthy();
  });

  it("shows Offline when the transport is down", () => {
    const tau = fakeTau({ transport: "down" });
    render(<LeftNav active="chat" unreadTotal={0} tau={tau} onNavigate={() => undefined} />);
    expect(screen.getByText("Offline")).toBeTruthy();
    expect(screen.queryByLabelText(/unread/)).toBeNull();
  });
});
