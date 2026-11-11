import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { OutboxEntry, RetryOutboxResponse } from "@/lib/types";

import { OutboxSection } from "./outbox-section";

function makeEntry(overrides: Partial<OutboxEntry> = {}): OutboxEntry {
  return {
    id: "aa".repeat(16),
    to: "ab".repeat(16),
    to_name: "alice",
    text: "hello alice",
    created_at: Date.now() / 1000 - 60,
    expires_at: Date.now() / 1000 + 3600,
    attempts: 1,
    last_attempt_at: null,
    last_error: null,
    held_by: [],
    ...overrides,
  };
}

function makeResult(
  overrides: Partial<RetryOutboxResponse> = {},
): RetryOutboxResponse {
  return {
    ok: true,
    delivered: 0,
    deposited: 0,
    received: 0,
    still_queued: 0,
    lock_busy: false,
    ...overrides,
  };
}

function renderSection(
  entries: OutboxEntry[],
  {
    onRetry = vi.fn(async () => makeResult()),
    onCancel = vi.fn(async () => undefined),
  }: {
    onRetry?: (id?: string) => Promise<RetryOutboxResponse>;
    onCancel?: (id: string) => Promise<unknown>;
  } = {},
) {
  return {
    onRetry,
    onCancel,
    ...render(
      <OutboxSection
        entries={entries}
        heldForOthers={0}
        held={[]}
        onCancel={onCancel}
        onRetry={onRetry}
        onDropHeld={vi.fn(async () => undefined)}
      />,
    ),
  };
}

describe("OutboxSection retry", () => {
  it("shows a retry button per entry and a retry-all button", () => {
    renderSection([makeEntry()]);

    expect(
      screen.getAllByRole("button", { name: "retry" }),
    ).toHaveLength(1);
    expect(
      screen.getByRole("button", { name: "retry all" }),
    ).toBeTruthy();
  });

  it("retries one entry by id and reports the delivery", async () => {
    const onRetry = vi.fn(async (id?: string) =>
      makeResult({
        delivered: 1,
        still_queued: 0,
        target: id,
        target_delivered: true,
        target_still_queued: false,
      }),
    );
    renderSection([makeEntry()], { onRetry });

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(onRetry).toHaveBeenCalledWith("aa".repeat(16));
    expect(
      await screen.findByText("delivered to alice · abababab…abab"),
    ).toBeTruthy();
  });

  it("reports a targeted retry that leaves the entry queued", async () => {
    const onRetry = vi.fn(async (id?: string) =>
      makeResult({
        delivered: 0,
        still_queued: 1,
        target: id,
        target_delivered: false,
        target_still_queued: true,
      }),
    );
    renderSection([makeEntry()], { onRetry });

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(
      await screen.findByText("still queued — peer unreachable"),
    ).toBeTruthy();
  });

  it("does not guess why an entry left the queue unconfirmed", async () => {
    const onRetry = vi.fn(async (id?: string) =>
      makeResult({
        delivered: 0,
        still_queued: 0,
        target: id,
        target_delivered: false,
        target_still_queued: false,
      }),
    );
    renderSection([makeEntry()], { onRetry });

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(
      await screen.findByText("left the queue — delivery not confirmed"),
    ).toBeTruthy();
  });

  it("retries everything and reports the summary", async () => {
    const onRetry = vi.fn(async () =>
      makeResult({ delivered: 1, deposited: 1, still_queued: 1 }),
    );
    renderSection(
      [makeEntry(), makeEntry({ id: "bb".repeat(16), to_name: "bob" })],
      { onRetry },
    );

    fireEvent.click(screen.getByRole("button", { name: "retry all" }));

    expect(onRetry).toHaveBeenCalledWith(undefined);
    expect(
      await screen.findByText("delivered 1 · deposited 1 · still queued 1"),
    ).toBeTruthy();
  });

  it("shows a retry failure instead of a result", async () => {
    const onRetry = vi.fn(async () => {
      throw new Error("a mail round is already running; try again shortly");
    });
    renderSection([makeEntry()], { onRetry });

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(
      await screen.findByText(
        "a mail round is already running; try again shortly",
      ),
    ).toBeTruthy();
  });

  it("renders nothing when the queue is empty and there is no feedback", () => {
    const { container } = renderSection([]);

    expect(container.textContent).toBe("");
  });

  it("disables cancel while a retry is in flight", () => {
    const onRetry = vi.fn(() => new Promise<RetryOutboxResponse>(() => {}));
    renderSection([makeEntry()], { onRetry });

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(screen.getByRole("button", { name: "retrying…" })).toBeTruthy();
    expect(
      (screen.getByRole("button", { name: "cancel" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("disables retry while a cancel is in flight", () => {
    const onCancel = vi.fn(() => new Promise<unknown>(() => {}));
    renderSection([makeEntry()], { onCancel });

    fireEvent.click(screen.getByRole("button", { name: "cancel" }));
    fireEvent.click(screen.getByRole("button", { name: "confirm cancel" }));

    expect(onCancel).toHaveBeenCalledWith("aa".repeat(16));
    expect(
      (screen.getByRole("button", { name: "retry" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "retry all" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("does not confirm a cancel while a retry is in flight", () => {
    const onRetry = vi.fn(() => new Promise<RetryOutboxResponse>(() => {}));
    const onCancel = vi.fn(async () => undefined);
    renderSection([makeEntry()], { onRetry, onCancel });

    fireEvent.click(screen.getByRole("button", { name: "cancel" }));
    fireEvent.click(screen.getByRole("button", { name: "retry" }));
    // The confirm click lands while the retry is running: the guard
    // drops it instead of racing the delivery.
    fireEvent.click(screen.getByRole("button", { name: "confirm cancel" }));

    expect(onCancel).not.toHaveBeenCalled();
  });
});
