// MessageSearch.test.tsx — search box calls the messages API, shows
// results with conversation + time, opens a conversation on click, and
// clears back to the conversation list.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import MessageSearch from "./MessageSearch";
import { makeMessage } from "../test-utils";
import type { ChatMessage } from "@/lib/types";

vi.mock("@/lib/api", () => ({ searchMessages: vi.fn() }));

import { searchMessages } from "@/lib/api";

const mockedSearch = vi.mocked(searchMessages);
const NOW = 1700000100;

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

function hit(overrides: Partial<ChatMessage> = {}): ChatMessage {
  return makeMessage({
    id: 1,
    text: "meet at the park at noon",
    peer: PEER_HASH,
    peer_name: "Alice",
    sent_at: NOW - 3600,
    received_at: NOW - 3600,
    ...overrides,
  });
}

function renderSearch(props: Partial<React.ComponentProps<typeof MessageSearch>> = {}) {
  const onOpen = vi.fn();
  const onActiveChange = vi.fn();
  const utils = render(
    <MessageSearch onOpen={onOpen} onActiveChange={onActiveChange} now={NOW} {...props} />,
  );
  return { ...utils, onOpen, onActiveChange };
}

describe("MessageSearch", () => {
  it("does not search on an empty query", async () => {
    mockedSearch.mockResolvedValue([]);
    renderSearch();
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(mockedSearch).not.toHaveBeenCalled();
    expect(screen.queryByText(/result/)).toBeNull();
  });

  it("shows results with conversation, text and time", async () => {
    mockedSearch.mockResolvedValue([
      hit(),
      hit({
        id: 2,
        text: "park tickets are booked",
        group: "0123456789abcdef0123456789abcdef",
        group_name: "Hikers",
        peer: null,
        peer_name: "",
        sent_at: NOW - 60,
        received_at: NOW - 60,
      }),
    ]);
    const { onActiveChange } = renderSearch();
    fireEvent.change(screen.getByLabelText("Search messages"), {
      target: { value: "park" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await waitFor(() => expect(mockedSearch).toHaveBeenCalledWith("park"));
    expect(await screen.findByText("2 results for “park”")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(screen.getByText("meet at the park at noon")).toBeTruthy();
    expect(screen.getByText("Hikers")).toBeTruthy();
    expect(screen.getByText("park tickets are booked")).toBeTruthy();
    expect(onActiveChange).toHaveBeenCalledWith(true);
  });

  it("says so plainly when nothing matches", async () => {
    mockedSearch.mockResolvedValue([]);
    renderSearch();
    fireEvent.change(screen.getByLabelText("Search messages"), {
      target: { value: "zzz" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByText("No messages match that search.")).toBeTruthy();
  });

  it("reports a failed search instead of showing stale results", async () => {
    mockedSearch.mockRejectedValue(new Error("node unreachable"));
    renderSearch();
    fireEvent.change(screen.getByLabelText("Search messages"), {
      target: { value: "park" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("node unreachable");
    expect(screen.queryByText(/results for/)).toBeNull();
  });

  it("opens the result's conversation on click", async () => {
    mockedSearch.mockResolvedValue([hit()]);
    const { onOpen } = renderSearch();
    fireEvent.change(screen.getByLabelText("Search messages"), {
      target: { value: "park" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    const row = await screen.findByRole("button", {
      name: "Open conversation with Alice",
    });
    fireEvent.click(row);
    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(onOpen.mock.calls[0][0].id).toBe(1);
  });

  it("clear returns to the conversation list", async () => {
    mockedSearch.mockResolvedValue([hit()]);
    const { onActiveChange } = renderSearch();
    fireEvent.change(screen.getByLabelText("Search messages"), {
      target: { value: "park" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await screen.findByText("1 result for “park”");
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.queryByText("meet at the park at noon")).toBeNull();
    expect(onActiveChange).toHaveBeenCalledWith(false);
  });
});
