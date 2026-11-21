// BroadcastsPanel.test.tsx — create / send / delete flows of the
// broadcast lists panel, with the api layer stubbed out.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import BroadcastsPanel from "./BroadcastsPanel";
import { makePeer } from "../test-utils";
import type { BroadcastList, BroadcastSendResponse } from "@/lib/types";

const ALICE = "a".repeat(32);
const BOB = "b".repeat(32);

const TEAM: BroadcastList = {
  name: "team",
  members: [
    { hash: ALICE, name: "Alice" },
    { hash: BOB, name: "Bob" },
  ],
  created_at: 1700000000,
};

function renderPanel(
  overrides: Partial<Parameters<typeof BroadcastsPanel>[0]> = {},
) {
  const props = {
    broadcasts: [TEAM],
    peers: [makePeer({ destination: ALICE, name: "Alice" }), makePeer({ destination: BOB, name: "Bob" })],
    onCreate: vi.fn(async () => undefined),
    onDelete: vi.fn(async () => undefined),
    onRename: vi.fn(async () => undefined),
    onSend: vi.fn(async (): Promise<BroadcastSendResponse> => ({
      ok: true,
      name: "team",
      results: [
        { hash: ALICE, name: "Alice", status: "delivered" },
        { hash: BOB, name: "Bob", status: "queued" },
      ],
      delivered: 1,
      queued: 1,
      failed: 0,
    })),
    ...overrides,
  };
  render(<BroadcastsPanel {...props} />);
  return props;
}

describe("BroadcastsPanel", () => {
  it("lists saved lists with member names", () => {
    renderPanel();
    expect(screen.getByText("team (2)")).toBeTruthy();
    expect(screen.getByText("Alice, Bob")).toBeTruthy();
  });

  it("sends typed text and shows per-member results", async () => {
    const props = renderPanel();
    fireEvent.change(screen.getByLabelText("Message team"), {
      target: { value: "meet at noon" },
    });
    fireEvent.click(screen.getByText("Send to 2"));
    expect(props.onSend).toHaveBeenCalledWith("team", "meet at noon");
    await vi.waitFor(() => {
      expect(screen.getByText("Alice: delivered")).toBeTruthy();
      expect(screen.getByText("Bob: queued")).toBeTruthy();
    });
    expect(
      (screen.getByLabelText("Message team") as HTMLTextAreaElement).value,
    ).toBe("");
  });

  it("keeps Send disabled until there is text", () => {
    renderPanel();
    expect(
      (screen.getByText("Send to 2") as HTMLButtonElement).disabled,
    ).toBe(true);
  });

  it("shows a failed member with its reason", async () => {
    const props = renderPanel({
      onSend: vi.fn(async (): Promise<BroadcastSendResponse> => ({
        ok: true,
        name: "team",
        results: [
          { hash: ALICE, name: "Alice", status: "delivered" },
          {
            hash: BOB,
            name: "Bob",
            status: "failed",
            error: "unknown peer: " + BOB,
          },
        ],
        delivered: 1,
        queued: 0,
        failed: 1,
      })),
    });
    fireEvent.change(screen.getByLabelText("Message team"), {
      target: { value: "hi" },
    });
    fireEvent.click(screen.getByText("Send to 2"));
    await vi.waitFor(() => {
      expect(
        screen.getByText(`Bob: failed — unknown peer: ${BOB}`),
      ).toBeTruthy();
    });
    expect(props.onSend).toHaveBeenCalledTimes(1);
  });

  it("creates a list from the form with picked peers", async () => {
    const props = renderPanel({ broadcasts: [] });
    fireEvent.click(screen.getByText("New list"));
    fireEvent.change(screen.getByLabelText("Broadcast list name"), {
      target: { value: "team" },
    });
    fireEvent.click(screen.getByLabelText("Alice"));
    fireEvent.click(screen.getByText("Create list"));
    await vi.waitFor(() => {
      expect(props.onCreate).toHaveBeenCalledWith("team", [ALICE]);
    });
  });

  it("refuses to create a list with no members picked", async () => {
    const props = renderPanel({ broadcasts: [] });
    fireEvent.click(screen.getByText("New list"));
    fireEvent.change(screen.getByLabelText("Broadcast list name"), {
      target: { value: "team" },
    });
    fireEvent.click(screen.getByText("Create list"));
    expect(screen.getByRole("alert").textContent).toMatch(/at least one peer/);
    expect(props.onCreate).not.toHaveBeenCalled();
  });

  it("asks twice before deleting a list", async () => {
    const props = renderPanel();
    fireEvent.click(screen.getByText("Delete"));
    expect(props.onDelete).not.toHaveBeenCalled();
    fireEvent.click(screen.getByText("Confirm delete"));
    await vi.waitFor(() => {
      expect(props.onDelete).toHaveBeenCalledWith("team");
    });
  });

  it("renames a list from the inline editor", async () => {
    const props = renderPanel();
    fireEvent.click(screen.getByText("Rename"));
    const input = screen.getByLabelText("Rename team") as HTMLInputElement;
    expect(input.value).toBe("team");
    fireEvent.change(input, { target: { value: "squad" } });
    fireEvent.click(screen.getByText("Save"));
    await vi.waitFor(() => {
      expect(props.onRename).toHaveBeenCalledWith("team", "squad");
    });
  });

  it("does not rename when the name is unchanged", async () => {
    const props = renderPanel();
    fireEvent.click(screen.getByText("Rename"));
    fireEvent.click(screen.getByText("Save"));
    expect(props.onRename).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("Rename team")).toBeNull();
  });

  it("says so when there are no lists yet", () => {
    renderPanel({ broadcasts: [] });
    expect(screen.getByText(/No broadcast lists/)).toBeTruthy();
  });
});
