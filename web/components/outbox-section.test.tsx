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
  onRetry = vi.fn(async () => makeResult()),
) {
  return {
    onRetry,
    ...render(
      <OutboxSection
        entries={entries}
        heldForOthers={0}
        onCancel={vi.fn(async () => undefined)}
        onRetry={onRetry}
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

  it("retries one entry and reports the delivery", async () => {
    const onRetry = vi.fn(async (id?: string) =>
      makeResult({
        delivered: 1,
        still_queued: 0,
        target: id,
        target_delivered: true,
        target_still_queued: false,
      }),
    );
    renderSection([makeEntry()], onRetry);

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
    renderSection([makeEntry()], onRetry);

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(
      await screen.findByText("still queued — peer unreachable"),
    ).toBeTruthy();
  });

  it("retries everything and reports the summary", async () => {
    const onRetry = vi.fn(async () =>
      makeResult({ delivered: 1, deposited: 1, still_queued: 1 }),
    );
    renderSection(
      [makeEntry(), makeEntry({ id: "bb".repeat(16), to_name: "bob" })],
      onRetry,
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
    renderSection([makeEntry()], onRetry);

    fireEvent.click(screen.getByRole("button", { name: "retry" }));

    expect(
      await screen.findByText(
        "a mail round is already running; try again shortly",
      ),
    ).toBeTruthy();
  });
});
