import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  DecideJoinRequestResponse,
  JoinRequestEntry,
} from "@/lib/types";

import { JoinRequestsSection } from "./join-requests-section";

function makeRequest(
  overrides: Partial<JoinRequestEntry> = {},
): JoinRequestEntry {
  const now = Date.now() / 1000;
  return {
    group: "crew",
    group_id: "ab".repeat(16),
    peer: "cd".repeat(16),
    peer_name: "carol",
    requested_at: now - 60,
    expires_at: now + 7 * 24 * 60 * 60,
    ...overrides,
  };
}

function makeDecideResult(
  overrides: Partial<DecideJoinRequestResponse> = {},
): DecideJoinRequestResponse {
  return {
    ok: true,
    status: "added",
    group: {
      id: "ab".repeat(16),
      name: "crew",
      members: ["cd".repeat(16)],
      creator: "ef".repeat(16),
    },
    ...overrides,
  };
}

function renderSection(
  requests: JoinRequestEntry[],
  {
    onApprove = vi.fn(async () => makeDecideResult()),
    onDeny = vi.fn(async () => makeDecideResult({ status: "denied" })),
  }: {
    onApprove?: (
      group: string,
      peer: string,
    ) => Promise<DecideJoinRequestResponse>;
    onDeny?: (
      group: string,
      peer: string,
    ) => Promise<DecideJoinRequestResponse>;
  } = {},
) {
  return {
    onApprove,
    onDeny,
    ...render(
      <JoinRequestsSection
        requests={requests}
        onApprove={onApprove}
        onDeny={onDeny}
      />,
    ),
  };
}

describe("JoinRequestsSection", () => {
  it("renders nothing when no requests are pending", () => {
    const { container } = renderSection([]);

    expect(container.textContent).toBe("");
  });

  it("shows the requester and group with approve and deny buttons", () => {
    renderSection([makeRequest()]);

    expect(screen.getByText(/carol/)).toBeTruthy();
    expect(screen.getByText(/wants to join crew/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "approve" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "deny" })).toBeTruthy();
  });

  it("approves at once and reports the join", async () => {
    const { onApprove } = renderSection([makeRequest()]);

    fireEvent.click(screen.getByRole("button", { name: "approve" }));

    expect(onApprove).toHaveBeenCalledWith("crew", "cd".repeat(16));
    await screen.findByText(/carol joined crew/);
  });

  it("denies only after a confirm click", async () => {
    const { onDeny } = renderSection([makeRequest()]);

    fireEvent.click(screen.getByRole("button", { name: "deny" }));
    expect(onDeny).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "confirm deny" }));
    expect(onDeny).toHaveBeenCalledWith("crew", "cd".repeat(16));
    await screen.findByText(/carol was denied for crew/);
  });
});
