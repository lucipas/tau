import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  AcceptDmRequestResponse,
  DeclineDmRequestResponse,
  DmRequestEntry,
} from "@/lib/types";

import { DmRequestsSection } from "./dm-requests-section";

function makeRequest(overrides: Partial<DmRequestEntry> = {}): DmRequestEntry {
  const now = Date.now() / 1000;
  return {
    id: "req-1",
    sender: "ab".repeat(16),
    sender_name: "alice",
    text: "hello, let me in",
    kind: "message",
    sent_at: now - 60,
    received_at: now - 60,
    request_expires_at: now + 7 * 24 * 60 * 60,
    ...overrides,
  };
}

function makeAcceptResult(
  overrides: Partial<AcceptDmRequestResponse> = {},
): AcceptDmRequestResponse {
  return {
    accepted: makeRequest(),
    delivered: true,
    allowlisted: true,
    skipped: null,
    ...overrides,
  };
}

function renderSection(
  requests: DmRequestEntry[],
  {
    onAccept = vi.fn(async () => makeAcceptResult()),
    onDecline = vi.fn(async () => ({ declined: makeRequest() })),
  }: {
    onAccept?: (id: string) => Promise<AcceptDmRequestResponse>;
    onDecline?: (id: string) => Promise<DeclineDmRequestResponse>;
  } = {},
) {
  return {
    onAccept,
    onDecline,
    ...render(
      <DmRequestsSection
        requests={requests}
        onAccept={onAccept}
        onDecline={onDecline}
      />,
    ),
  };
}

describe("DmRequestsSection", () => {
  it("renders nothing when no requests are held", () => {
    const { container } = renderSection([]);

    expect(container.textContent).toBe("");
  });

  it("shows the sender and message with accept and decline buttons", () => {
    renderSection([makeRequest()]);

    expect(screen.getByText(/alice/)).toBeTruthy();
    expect(screen.getByText("hello, let me in")).toBeTruthy();
    expect(screen.getByRole("button", { name: "accept" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "decline" })).toBeTruthy();
  });

  it("accepts with one click and reports the delivery", async () => {
    const { onAccept } = renderSection([makeRequest()]);

    fireEvent.click(screen.getByRole("button", { name: "accept" }));
    expect(onAccept).toHaveBeenCalledWith("req-1");
    expect(
      await screen.findByText(/sender allow-listed, message delivered/),
    ).toBeTruthy();
  });

  it("asks for confirmation before declining", async () => {
    const { onDecline } = renderSection([makeRequest()]);

    fireEvent.click(screen.getByRole("button", { name: "decline" }));
    expect(onDecline).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "confirm decline" }));
    expect(onDecline).toHaveBeenCalledWith("req-1");
    expect(
      await screen.findByText(/the held message was dropped/),
    ).toBeTruthy();
  });

  it("shows an accept failure instead of the notice", async () => {
    renderSection([makeRequest()], {
      onAccept: vi.fn(async () => {
        throw new Error("sender is blocked");
      }),
    });

    fireEvent.click(screen.getByRole("button", { name: "accept" }));
    expect(await screen.findByText("sender is blocked")).toBeTruthy();
  });

  it("clears the old notice when a later accept fails", async () => {
    const onAccept = vi
      .fn()
      .mockResolvedValueOnce(makeAcceptResult())
      .mockRejectedValueOnce(new Error("sender is blocked"));
    renderSection(
      [makeRequest(), makeRequest({ id: "req-2", sender: "cd".repeat(16) })],
      { onAccept },
    );

    fireEvent.click(screen.getAllByRole("button", { name: "accept" })[0]);
    expect(
      await screen.findByText(/sender allow-listed, message delivered/),
    ).toBeTruthy();

    fireEvent.click(screen.getAllByRole("button", { name: "accept" })[1]);
    expect(await screen.findByText("sender is blocked")).toBeTruthy();
    expect(
      screen.queryByText(/sender allow-listed, message delivered/),
    ).toBeNull();
  });
});
