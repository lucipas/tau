"use client";

import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { useChannels } from "@/lib/use-channels";
import type { UseTauResult } from "@/lib/use-tau";
import { ChannelsTab } from "./channels-tab";

function fakeTau(): UseTauResult {
  return {} as unknown as UseTauResult;
}

function fakeChannels(
  overrides: Partial<ReturnType<typeof useChannels>> = {},
): ReturnType<typeof useChannels> {
  return {
    channels: [
      { channel: "general", joined_at: 1700000000 },
      { channel: "mesh", joined_at: 1700001000 },
    ],
    selected: "general",
    messages: [
      {
        v: 1,
        hash: "hash-aaa-1",
        channel: "general",
        author: "authorhash1111111111",
        key: "key-1",
        name: "Alice",
        text: "hello from the mesh",
        created_at: 1700002000,
        received_at: 1700002001,
        sig: "sig-1",
      },
    ],
    loading: false,
    error: null,
    sending: false,
    syncing: false,
    joining: false,
    select: vi.fn(),
    join: vi.fn(),
    leave: vi.fn(),
    send: vi.fn(),
    sync: vi.fn(),
    ...overrides,
  } as unknown as ReturnType<typeof useChannels>;
}

describe("ChannelsTab", () => {
  it("renders the channel list and the join form", () => {
    render(<ChannelsTab tau={fakeTau()} channels={fakeChannels()} />);
    expect(screen.getAllByText("#general")).toHaveLength(2); // list + open channel title
    expect(screen.getByText("#mesh")).toBeTruthy();
    expect(
      screen.getByPlaceholderText("channel name"),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "join" })).toBeTruthy();
  });

  it("renders the selected channel messages oldest first with a composer", () => {
    render(<ChannelsTab tau={fakeTau()} channels={fakeChannels()} />);
    expect(screen.getByText("hello from the mesh")).toBeTruthy();
    expect(screen.getByText("Alice")).toBeTruthy();
    expect(
      screen.getByPlaceholderText("message #general"),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "send" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "sync" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "leave" })).toBeTruthy();
  });

  it("shows an empty state when no channel is selected", () => {
    render(
      <ChannelsTab
        tau={fakeTau()}
        channels={fakeChannels({ selected: null, messages: [] })}
      />,
    );
    expect(
      screen.getByText("Select a channel above to read its messages."),
    ).toBeTruthy();
    expect(
      screen.queryByPlaceholderText("message #general"),
    ).toBeNull();
  });

  it("shows an empty state when no channels are joined", () => {
    render(
      <ChannelsTab
        tau={fakeTau()}
        channels={fakeChannels({ channels: [], selected: null, messages: [] })}
      />,
    );
    expect(
      screen.getByText("No channels joined yet — join one by name above."),
    ).toBeTruthy();
  });
});
