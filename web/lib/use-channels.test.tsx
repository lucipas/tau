import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { Channel, ChannelMessage } from "./types";
import { useChannels } from "./use-channels";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

function makeChannel(overrides: Partial<Channel> = {}): Channel {
  return {
    channel: "general",
    joined_at: 1000,
    ...overrides,
  };
}

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

function routeLoads(
  channels: Channel[],
  historyByChannel: Record<string, ChannelMessage[]>,
) {
  mockedGet.mockImplementation((path: string) => {
    if (path === "/api/channels") {
      return Promise.resolve({ channels, count: channels.length });
    }
    if (path.startsWith("/api/channels/messages")) {
      const channel = new URLSearchParams(path.split("?")[1]).get("channel");
      const messages = (channel && historyByChannel[channel]) ?? [];
      return Promise.resolve({ channel, messages, count: messages.length });
    }
    return Promise.reject(new Error(`unexpected GET ${path}`));
  });
  mockedPost.mockResolvedValue({ ok: true });
}

describe("useChannels", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("loads joined channels, selects the first, and shows its history oldest-first", async () => {
    const older = makeMessage({ hash: "a".repeat(64), text: "first", created_at: 1000 });
    const newer = makeMessage({ hash: "b".repeat(64), text: "second", created_at: 1001 });
    routeLoads(
      [makeChannel({ channel: "general" }), makeChannel({ channel: "random", joined_at: 900 })],
      // The API returns newest first; the hook reverses for display.
      { general: [newer, older] },
    );

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() =>
      expect(result.current.messages).toEqual([older, newer]),
    );
    expect(result.current.channels.map((entry) => entry.channel)).toEqual([
      "general",
      "random",
    ]);
    expect(result.current.selected).toBe("general");
    expect(mockedGet).toHaveBeenCalledWith(
      "/api/channels/messages?channel=general&limit=50",
    );
  });

  it("loads no history when no channel is joined", async () => {
    routeLoads([], {});

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.channels).toEqual([]);
    expect(result.current.selected).toBeNull();
    expect(result.current.messages).toEqual([]);
    expect(
      mockedGet.mock.calls.some((call) =>
        String(call[0]).startsWith("/api/channels/messages"),
      ),
    ).toBe(false);
  });

  it("appends a live message for the open channel and ignores the rest", async () => {
    const stored = makeMessage({ hash: "a".repeat(64), text: "first" });
    routeLoads([makeChannel()], { general: [stored] });

    const { result, rerender } = renderHook(
      ({ live }: { live: ChannelMessage | null }) => useChannels(live),
      { initialProps: { live: null as ChannelMessage | null } },
    );
    await waitFor(() =>
      expect(result.current.messages).toEqual([stored]),
    );

    const live = makeMessage({ hash: "b".repeat(64), text: "live one" });
    rerender({ live });
    await waitFor(() =>
      expect(result.current.messages).toEqual([stored, live]),
    );

    // The same hash in a fresh object is a replay, not a second message.
    rerender({ live: { ...live } });
    await waitFor(() =>
      expect(result.current.messages).toEqual([stored, live]),
    );

    // A message for another channel never lands in this history.
    rerender({
      live: makeMessage({
        hash: "e".repeat(64),
        channel: "random",
        text: "elsewhere",
      }),
    });
    await waitFor(() =>
      expect(result.current.messages).toEqual([stored, live]),
    );
  });

  it("joins a channel by name and selects it", async () => {
    routeLoads([makeChannel()], { general: [] });
    mockedPost.mockImplementation((path: string) => {
      if (path === "/api/channels/join") {
        return Promise.resolve({ ok: true, channel: "random", joined_at: 2000 });
      }
      return Promise.resolve({ ok: true });
    });

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.join("random");
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/channels/join", {
      channel: "random",
    });
    expect(result.current.selected).toBe("random");
  });

  it("sends to the open channel and appends the stored reply", async () => {
    const sent = makeMessage({ hash: "b".repeat(64), text: "hello all" });
    routeLoads([makeChannel()], { general: [] });
    mockedPost.mockImplementation((path: string) => {
      if (path === "/api/channels/send") {
        return Promise.resolve({ ok: true, message: sent });
      }
      return Promise.resolve({ ok: true });
    });

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.send("hello all");
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/channels/send", {
      channel: "general",
      text: "hello all",
    });
    expect(result.current.messages).toEqual([sent]);
  });

  it("leaves a channel and reloads the list", async () => {
    routeLoads([makeChannel()], { general: [] });

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.leave("general");
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/channels/leave", {
      channel: "general",
    });
  });

  it("syncs the open channel and returns the summary", async () => {
    routeLoads([makeChannel()], { general: [] });
    const summary = {
      channel: "general",
      peers: 2,
      stored: 3,
      duplicates: 0,
      skipped: 0,
      errors: [],
    };
    mockedPost.mockImplementation((path: string) => {
      if (path === "/api/channels/sync") {
        return Promise.resolve({ ok: true, sync: summary });
      }
      return Promise.resolve({ ok: true });
    });

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));

    let returned: unknown;
    await act(async () => {
      returned = await result.current.sync();
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/channels/sync", {
      channel: "general",
    });
    expect(returned).toEqual(summary);
  });

  it("surfaces a failed load instead of spinning forever", async () => {
    mockedGet.mockRejectedValue(new Error("no identity; run `tau init` first"));
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useChannels(null));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.error).toBe("no identity; run `tau init` first");
    expect(result.current.channels).toEqual([]);
  });
});
