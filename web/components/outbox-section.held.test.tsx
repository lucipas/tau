import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { HeldMailEntry, RetryOutboxResponse } from "@/lib/types";

import { OutboxSection } from "./outbox-section";

function makeHeld(overrides: Partial<HeldMailEntry> = {}): HeldMailEntry {
  return {
    id: "cc" + "11".repeat(15),
    to: "ab".repeat(16),
    to_name: "alice",
    from: "cd".repeat(16),
    from_name: "bob",
    stored_at: Date.now() / 1000 - 60,
    expires_at: Date.now() / 1000 + 3600,
    blob_bytes: 512,
    ...overrides,
  };
}

const noop = () => Promise.resolve();

const retryNoop = async (): Promise<RetryOutboxResponse> => ({
  ok: true,
  delivered: 0,
  deposited: 0,
  received: 0,
  still_queued: 0,
  lock_busy: false,
});

function renderHeld(held: HeldMailEntry[], onDropHeld = noop) {
  return render(
    <OutboxSection
      entries={[]}
      heldForOthers={held.length}
      held={held}
      onCancel={noop}
      onRetry={retryNoop}
      onDropHeld={onDropHeld}
    />,
  );
}

describe("OutboxSection held mail", () => {
  it("lists held envelopes with recipient, sender and size", () => {
    renderHeld([makeHeld()]);

    expect(screen.getByText(/alice · /)).toBeTruthy();
    expect(screen.getByText(/from bob · /)).toBeTruthy();
    expect(screen.getByText(/512 B sealed/)).toBeTruthy();
  });

  it("drops a held envelope only after a second confirming click", async () => {
    const onDropHeld = vi.fn(() => Promise.resolve());
    renderHeld([makeHeld()], onDropHeld);

    fireEvent.click(screen.getByRole("button", { name: "drop" }));
    expect(onDropHeld).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "confirm drop" }));
    await waitFor(() =>
      expect(onDropHeld).toHaveBeenCalledWith("cc" + "11".repeat(15)),
    );
  });

  it("shows a drop error under the section", async () => {
    const onDropHeld = vi.fn(() =>
      Promise.reject(new Error("drop exploded")),
    );
    renderHeld([makeHeld()], onDropHeld);

    fireEvent.click(screen.getByRole("button", { name: "drop" }));
    fireEvent.click(screen.getByRole("button", { name: "confirm drop" }));
    expect(await screen.findByText("drop exploded")).toBeTruthy();
  });

  it("falls back to the held count line while the list is still empty", () => {
    render(
      <OutboxSection
        entries={[]}
        heldForOthers={2}
        held={[]}
        onCancel={noop}
        onRetry={retryNoop}
        onDropHeld={noop}
      />,
    );

    expect(
      screen.getByText(/also holding 2 sealed messages/),
    ).toBeTruthy();
  });
});
