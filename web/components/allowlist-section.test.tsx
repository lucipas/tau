import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { AllowlistResponse } from "@/lib/types";

import { AllowlistSection } from "./allowlist-section";

const PEER_HASH = "ab".repeat(16);
const OTHER_HASH = "cd".repeat(16);

function makeAllowlist(
  overrides: Partial<AllowlistResponse> = {},
): AllowlistResponse {
  return {
    enabled: false,
    entries: [
      { destination: PEER_HASH, name: "alice" },
      { destination: OTHER_HASH, name: "" },
    ],
    ...overrides,
  };
}

function renderSection(
  allowlist: AllowlistResponse | null,
  handlers: {
    onAdd?: (target: string) => Promise<unknown>;
    onRemove?: (target: string) => Promise<unknown>;
    onToggle?: (enabled: boolean) => Promise<unknown>;
  } = {},
) {
  const onAdd = handlers.onAdd ?? vi.fn();
  const onRemove = handlers.onRemove ?? vi.fn();
  const onToggle = handlers.onToggle ?? vi.fn();
  render(
    <AllowlistSection
      allowlist={allowlist}
      onAdd={onAdd}
      onRemove={onRemove}
      onToggle={onToggle}
    />,
  );
  return { onAdd, onRemove, onToggle };
}

describe("AllowlistSection", () => {
  it("shows the enforced state and the listed senders", () => {
    renderSection(makeAllowlist({ enabled: true }));

    expect(screen.getByText("enforced")).toBeTruthy();
    expect(screen.getByText(/alice · abababab/)).toBeTruthy();
    expect(screen.getByText(/unknown · cdcdcdcd/)).toBeTruthy();
  });

  it("shows the off state and no entries when the list is empty", () => {
    renderSection(makeAllowlist({ enabled: false, entries: [] }));

    expect(screen.getByText("off")).toBeTruthy();
    expect(screen.getByText("no senders listed.")).toBeTruthy();
  });

  it("adds the typed peer and clears the input", async () => {
    const onAdd = vi.fn().mockResolvedValue(undefined);
    renderSection(makeAllowlist({ entries: [] }), { onAdd });

    const input = screen.getByLabelText("peer to allow");
    fireEvent.change(input, { target: { value: "alice" } });
    fireEvent.click(screen.getByText("allow"));

    expect(onAdd).toHaveBeenCalledWith("alice");
    await screen.findByText("allow");
    expect((input as HTMLInputElement).value).toBe("");
  });

  it("removes an entry when its remove button is clicked", () => {
    const onRemove = vi.fn().mockResolvedValue(undefined);
    renderSection(makeAllowlist(), { onRemove });

    const buttons = screen.getAllByText("remove");
    fireEvent.click(buttons[0]);

    expect(onRemove).toHaveBeenCalledWith(PEER_HASH);
  });

  it("toggles enforcement off with one click", () => {
    const onToggle = vi.fn().mockResolvedValue(undefined);
    renderSection(makeAllowlist({ enabled: true }), { onToggle });

    fireEvent.click(screen.getByText("enforced"));

    expect(onToggle).toHaveBeenCalledWith(false);
  });

  it("asks for confirmation before enforcing an empty list", () => {
    const onToggle = vi.fn().mockResolvedValue(undefined);
    renderSection(makeAllowlist({ enabled: false, entries: [] }), {
      onToggle,
    });

    fireEvent.click(screen.getByText("off"));

    expect(onToggle).not.toHaveBeenCalled();
    expect(screen.getByText("confirm — nobody can DM")).toBeTruthy();
    expect(
      screen.getByText(/the list is empty — enforcing it means nobody/),
    ).toBeTruthy();

    fireEvent.click(screen.getByText("confirm — nobody can DM"));

    expect(onToggle).toHaveBeenCalledWith(true);
  });

  it("shows the error when adding an unknown peer", async () => {
    const onAdd = vi.fn().mockRejectedValue(new Error("unknown peer"));
    renderSection(makeAllowlist({ entries: [] }), { onAdd });

    fireEvent.change(screen.getByLabelText("peer to allow"), {
      target: { value: "nobody" },
    });
    fireEvent.click(screen.getByText("allow"));

    await screen.findByText("unknown peer");
  });

  it("shows a loading note before the list arrives", () => {
    renderSection(null);

    expect(screen.getByText("loading…")).toBeTruthy();
  });
});
