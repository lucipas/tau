import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

<<<<<<< HEAD
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
=======
import type { TauMessage } from "@/lib/types";

import { Composer } from "./composer";

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "out",
    peer: "ab".repeat(16),
    peer_name: "alice",
    text: "Sent file: notes.txt (5 bytes)",
    sent_at: 1000,
    received_at: 1001,
    status: "delivered",
    ...overrides,
  };
}

function renderComposer({
  onSendFile = vi.fn().mockResolvedValue(makeMessage()),
  onSend = vi.fn().mockResolvedValue(makeMessage()),
} = {}) {
  render(
    <Composer
      peers={[]}
      target={"ab".repeat(16)}
      mode="custom"
      onTargetChange={() => {}}
      onModeChange={() => {}}
      onSend={onSend}
      onSendFile={onSendFile}
    />,
  );
  return { onSend, onSendFile };
}

function attachFile(file: File) {
  const input = screen.getByLabelText("Attach a file");
  fireEvent.change(input, { target: { files: [file] } });
}

describe("Composer file attach", () => {
  it("shows the picked file and sends it to the selected peer", async () => {
    const { onSendFile } = renderComposer();
    const file = new File(["hello"], "notes.txt", { type: "text/plain" });
    attachFile(file);

    expect(screen.getByText(/notes\.txt/)).toBeTruthy();
    expect(screen.getByText(/5 B/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /send file/i }));
    await waitFor(() =>
      expect(onSendFile).toHaveBeenCalledWith("ab".repeat(16), file),
    );
    await waitFor(() =>
      expect(screen.queryByText(/notes\.txt/)).toBeNull(),
    );
  });

  it("refuses a file larger than 10 MiB without sending", async () => {
    const { onSendFile } = renderComposer();
    const file = new File(["x"], "big.bin", {
      type: "application/octet-stream",
    });
    Object.defineProperty(file, "size", { value: 10 * 1024 * 1024 + 1 });
    attachFile(file);

    expect(screen.getByRole("alert").textContent).toBe(
      "File is larger than 10 MiB.",
    );
    expect(onSendFile).not.toHaveBeenCalled();
  });

  it("still sends text when no file is attached", async () => {
    const { onSend } = renderComposer();
    const textarea = screen.getByPlaceholderText(/Write something/);
    fireEvent.change(textarea, { target: { value: "hello" } });

    fireEvent.click(screen.getByRole("button", { name: /^send$/i }));
    await waitFor(() =>
      expect(onSend).toHaveBeenCalledWith(
        "ab".repeat(16),
        "hello",
        undefined,
        undefined,
      ),
    );
>>>>>>> origin/main
  });
});
