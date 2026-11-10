import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { NodeSettings } from "@/lib/types";

import { SettingsSection } from "./settings-section";

const SETTINGS: NodeSettings = {
  accept_files: false,
  send_read_receipts: true,
};

function renderSection(
  settings: NodeSettings | null = SETTINGS,
  onToggle = vi.fn().mockResolvedValue(undefined),
) {
  return render(<SettingsSection settings={settings} onToggle={onToggle} />);
}

describe("SettingsSection", () => {
  it("shows the current state of each setting", () => {
    renderSection();

    expect(
      screen
        .getByRole("switch", { name: "accept file transfers" })
        .getAttribute("aria-checked"),
    ).toBe("false");
    expect(
      screen
        .getByRole("switch", { name: "send read receipts" })
        .getAttribute("aria-checked"),
    ).toBe("true");
  });

  it("toggles a setting off and on through the callback", async () => {
    const onToggle = vi.fn().mockResolvedValue(undefined);
    renderSection(SETTINGS, onToggle);

    fireEvent.click(
      screen.getByRole("switch", { name: "send read receipts" }),
    );
    await waitFor(() =>
      expect(onToggle).toHaveBeenCalledWith("send_read_receipts", false),
    );

    fireEvent.click(
      screen.getByRole("switch", { name: "accept file transfers" }),
    );
    await waitFor(() =>
      expect(onToggle).toHaveBeenCalledWith("accept_files", true),
    );
  });

  it("shows the error inline when a toggle fails", async () => {
    const onToggle = vi.fn().mockRejectedValue(new Error("no data directory"));
    renderSection(SETTINGS, onToggle);

    fireEvent.click(
      screen.getByRole("switch", { name: "accept file transfers" }),
    );
    await waitFor(() =>
      expect(screen.getByText("no data directory")).not.toBeNull(),
    );
  });

  it("disables the toggles while settings are still loading", () => {
    renderSection(null);

    expect(
      (
        screen.getByRole("switch", {
          name: "accept file transfers",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(
      (
        screen.getByRole("switch", {
          name: "send read receipts",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
  });
});
