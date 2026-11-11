import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { NodeInfo } from "@/lib/types";

import { Header } from "./header";

const node: NodeInfo = {
  name: "alice",
  destination: "ab".repeat(16),
  protocol: 1,
  version: "0.1.0",
  rns_version: "test",
  uptime: 12.5,
  peers: 1,
  interfaces: [],
};

function renderHeader(overrides: Record<string, unknown> = {}) {
  const onBackup = vi.fn();
  render(
    <Header
      node={node}
      connection="live"
      announcing={false}
      onAnnounce={() => {}}
      backingUp={false}
      backupError={null}
      onBackup={onBackup}
      {...overrides}
    />,
  );
  return { onBackup };
}

describe("Header backup button", () => {
  it("calls onBackup when the back up button is clicked", () => {
    const { onBackup } = renderHeader();

    fireEvent.click(screen.getByRole("button", { name: "back up" }));

    expect(onBackup).toHaveBeenCalledTimes(1);
  });

  it("disables the button and shows progress while a backup is running", () => {
    renderHeader({ backingUp: true });

    const button = screen.getByRole("button", { name: "backing up…" });
    expect((button as HTMLButtonElement).disabled).toBe(true);
  });

  it("disables the button without a node", () => {
    renderHeader({ node: null });

    expect(
      (screen.getByRole("button", { name: "back up" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("shows the backup error", () => {
    renderHeader({ backupError: "no data directory" });

    const alert = screen.getByRole("alert");
    expect(alert.textContent).toContain("no data directory");
  });

  it("shows no error when the last backup succeeded", () => {
    renderHeader({ backupError: null });

    expect(screen.queryByRole("alert")).toBeNull();
  });
});
