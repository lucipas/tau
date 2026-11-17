import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { Channel, ChannelMessage } from "@/lib/types";

import { ChannelsConsole } from "./channels-console";

const mockChannels = vi.hoisted(() => ({
  current: {} as Record<string, unknown>,
}));

vi.mock("@/lib/use-channels", () => ({
  useChannels: vi.fn(() => mockChannels.current),
  CHANNEL_POLL_INTERVAL_MS: 5000,
}));

import { useChannels } from "@/lib/use-channels";

const mockedUseChannels = useChannels as Mock;

function makeMessage(overrides: Partial<ChannelMessage> = {}): ChannelMessage {
  return {
    v: 1,
    hash: "c".repeat(64),
    channel: "general",
    author: "a".repeat(64),
    key: "b".repeat(128),
    name: "alice",
    text: "hello",
    created_at: 1000,
    received_at: 1001,
    sig: "d".repeat(128),
    ...overrides,
  };
}

function makeState(overrides: Record<string, unknown> = {}) {
  return {
    channels: [] as Channel[],
    selected: null as string | null,
    messages: [] as ChannelMessage[],
    loading: false,
    error: null as string | null,
    sending: false,
    syncing: false,
    joining: false,
    select: vi.fn(),
    join: vi.fn(async () => undefined),
    leave: vi.fn(async () => undefined),
    send: vi.fn(async () => undefined),
    sync: vi.fn(async () => null),
    ...overrides,
  };
}

describe("ChannelsConsole", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockChannels.current = makeState();
  });

  it("shows the empty state when no channel is joined", () => {
    render(<ChannelsConsole channelMessage={null} />);

    expect(
      screen.getByText("no channels joined yet — join one by name above."),
    ).toBeTruthy();
    expect(screen.getByLabelText("join a channel by name")).toBeTruthy();
  });

  it("lists joined channels and shows the selected one's history oldest-first", () => {
    const older = makeMessage({ hash: "a".repeat(64), text: "first", created_at: 1000 });
    const newer = makeMessage({ hash: "b".repeat(64), text: "second", created_at: 1001 });
    mockChannels.current = makeState({
      channels: [
        { channel: "general", joined_at: 1000 },
        { channel: "random", joined_at: 900 },
      ],
      selected: "general",
      messages: [older, newer],
    });

    render(<ChannelsConsole channelMessage={null} />);

    expect(screen.getByRole("button", { name: /#general/ })).toBeTruthy();
    expect(screen.getByRole("button", { name: /#random/ })).toBeTruthy();
    expect(screen.getByText("first")).toBeTruthy();
    expect(screen.getByText("second")).toBeTruthy();
    // History order: the oldest message comes first in the document.
    const html = document.body.innerHTML;
    expect(html.indexOf("first")).toBeLessThan(html.indexOf("second"));
  });

  it("selects a channel when its list entry is clicked", () => {
    const select = vi.fn();
    mockChannels.current = makeState({
      channels: [
        { channel: "general", joined_at: 1000 },
        { channel: "random", joined_at: 900 },
      ],
      selected: "general",
      select,
    });

    render(<ChannelsConsole channelMessage={null} />);

    fireEvent.click(screen.getByRole("button", { name: /#random/ }));
    expect(select).toHaveBeenCalledWith("random");
  });

  it("joins the typed channel name", async () => {
    const join = vi.fn(async () => undefined);
    mockChannels.current = makeState({ join });

    render(<ChannelsConsole channelMessage={null} />);

    fireEvent.change(screen.getByLabelText("join a channel by name"), {
      target: { value: "random" },
    });
    fireEvent.click(screen.getByRole("button", { name: "join" }));
    await waitFor(() => expect(join).toHaveBeenCalledWith("random"));
  });

  it("sends the draft to the open channel and clears it", async () => {
    const send = vi.fn(async () => undefined);
    mockChannels.current = makeState({
      channels: [{ channel: "general", joined_at: 1000 }],
      selected: "general",
      messages: [],
      send,
    });

    render(<ChannelsConsole channelMessage={null} />);

    const composer = screen.getByLabelText("send a message to #general");
    fireEvent.change(composer, { target: { value: "hello all" } });
    fireEvent.click(screen.getByRole("button", { name: "send" }));
    await waitFor(() => expect(send).toHaveBeenCalledWith("hello all"));
    expect((composer as HTMLTextAreaElement).value).toBe("");
  });

  it("pulls new history and reports how many messages arrived", async () => {
    const sync = vi.fn(async () => ({
      channel: "general",
      peers: 2,
      stored: 3,
      duplicates: 0,
      skipped: 0,
      errors: [],
    }));
    mockChannels.current = makeState({
      channels: [{ channel: "general", joined_at: 1000 }],
      selected: "general",
      messages: [],
      sync,
    });

    render(<ChannelsConsole channelMessage={null} />);

    fireEvent.click(screen.getByRole("button", { name: "pull new history" }));
    await waitFor(() => expect(sync).toHaveBeenCalledTimes(1));
    expect(
      screen.getByText("pulled 3 new messages from 2 peers"),
    ).toBeTruthy();
  });

  it("leaves the open channel", async () => {
    const leave = vi.fn(async () => undefined);
    mockChannels.current = makeState({
      channels: [{ channel: "general", joined_at: 1000 }],
      selected: "general",
      messages: [],
      leave,
    });

    render(<ChannelsConsole channelMessage={null} />);

    fireEvent.click(screen.getByRole("button", { name: "leave" }));
    await waitFor(() => expect(leave).toHaveBeenCalledWith("general"));
  });

  it("shows the hook's error, e.g. channels not enabled on the node", () => {
    mockChannels.current = makeState({
      error: "public channels are not enabled; run serve --channels",
    });

    render(<ChannelsConsole channelMessage={null} />);

    expect(
      screen.getByRole("alert").textContent,
    ).toContain("public channels are not enabled");
  });

  it("passes the live channel message into the hook", () => {
    const live = makeMessage({ hash: "b".repeat(64), text: "live one" });
    mockChannels.current = makeState({
      channels: [{ channel: "general", joined_at: 1000 }],
      selected: "general",
    });

    render(<ChannelsConsole channelMessage={live} />);

    expect(mockedUseChannels).toHaveBeenCalledWith(live);
  });
});
