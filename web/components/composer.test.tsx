import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Peer, TauGroup, TauMessage } from "@/lib/types";

import { Composer } from "./composer";

const PEER_HASH = "ab".repeat(16);
const GROUP_ID = "a".repeat(32);

const peers: Peer[] = [
  {
    destination: PEER_HASH,
    name: "alice",
    last_seen: 1000,
    age: 0,
    hops: 1,
    protocol: 1,
  },
];

const groups: TauGroup[] = [
  {
    id: GROUP_ID,
    name: "crew",
    members: [
      { hash: PEER_HASH, name: "alice" },
      { hash: "ef".repeat(16), name: "bob" },
    ],
    created_at: 1000,
  },
];

function makeMessage(): TauMessage {
  return {
    id: 1,
    direction: "out",
    peer: PEER_HASH,
    peer_name: "alice",
    text: "hi all",
    sent_at: 1000,
    received_at: 1001,
    status: "delivered",
    group: GROUP_ID,
    group_name: "crew",
  };
}

describe("Composer group mode", () => {
  it("offers a group target from the peer picker", () => {
    const onModeChange = vi.fn();
    const onTargetChange = vi.fn();
    render(
      <Composer
        peers={peers}
        groups={groups}
        target={PEER_HASH}
        mode="peer"
        onTargetChange={onTargetChange}
        onModeChange={onModeChange}
        onSend={vi.fn()}
        onSendGroup={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByDisplayValue(`alice · ${PEER_HASH.slice(0, 8)}`), {
      target: { value: "__group" },
    });
    expect(onModeChange).toHaveBeenCalledWith("group");
    expect(onTargetChange).toHaveBeenCalledWith(GROUP_ID);
  });

  it("sends to the group and hides reply and expiry controls", async () => {
    const onSend = vi.fn();
    const onSendGroup = vi.fn().mockResolvedValue([makeMessage()]);
    render(
      <Composer
        peers={peers}
        groups={groups}
        target={GROUP_ID}
        mode="group"
        onTargetChange={vi.fn()}
        onModeChange={vi.fn()}
        onSend={onSend}
        onSendGroup={onSendGroup}
      />,
    );

    // The group picker lists the group with its member count.
    expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy();
    expect(screen.getByPlaceholderText("Write something to the group…")).toBeTruthy();
    // Replies and expiry are peer-DM-only; the picker says so instead.
    expect(screen.queryByLabelText("Message expiry")).toBeNull();
    expect(screen.getByText("replies and expiry are peer DMs only")).toBeTruthy();

    fireEvent.change(screen.getByPlaceholderText("Write something to the group…"), {
      target: { value: "hi all" },
    });
    fireEvent.click(screen.getByRole("button", { name: /send/ }));

    await waitFor(() => expect(onSendGroup).toHaveBeenCalledTimes(1));
    expect(onSendGroup).toHaveBeenCalledWith(GROUP_ID, "hi all");
    expect(onSend).not.toHaveBeenCalled();
  });

  it("hides the group option when there are no groups", () => {
    render(
      <Composer
        peers={peers}
        groups={[]}
        target={PEER_HASH}
        mode="peer"
        onTargetChange={vi.fn()}
        onModeChange={vi.fn()}
        onSend={vi.fn()}
        onSendGroup={vi.fn()}
      />,
    );

    expect(screen.queryByText("group chat…")).toBeNull();
  });
});
