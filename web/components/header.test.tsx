import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { NodeInfo } from "@/lib/types";

import { Header } from "./header";

function makeNode(overrides: Partial<NodeInfo> = {}): NodeInfo {
  return {
    name: "tester",
    destination: "cd".repeat(16),
    protocol: 1,
    version: "0.1.0",
    rns_version: "test",
    uptime: 12.5,
    peers: 1,
    interfaces: [{ name: "TCP", online: true }],
    ...overrides,
  };
}

function renderHeader(
  onRename: (name: string) => Promise<void> = () => Promise.resolve(),
  overrides: Record<string, unknown> = {},
) {
  const onBackup = vi.fn();
  render(
    <Header
      node={makeNode()}
      connection="live"
      announcing={false}
      onAnnounce={() => {}}
      onRename={onRename}
      backingUp={false}
      backupError={null}
      onBackup={onBackup}
      {...overrides}
    />,
  );
  return { onBackup };
}

describe("Header rename", () => {
  it("opens an edit field with the current name and saves the new one", async () => {
    const onRename = vi.fn().mockResolvedValue(undefined);
    renderHeader(onRename);

    fireEvent.click(screen.getByTitle("rename this node"));
    const input = screen.getByLabelText("node name");
    expect((input as HTMLInputElement).value).toBe("tester");

    fireEvent.change(input, { target: { value: "renamed" } });
    await act(async () => {
      fireEvent.click(screen.getByText("save"));
    });

    expect(onRename).toHaveBeenCalledWith("renamed");
    expect(screen.queryByLabelText("node name")).toBeNull();
  });

  it("does not call rename for an unchanged name", async () => {
    const onRename = vi.fn().mockResolvedValue(undefined);
    renderHeader(onRename);

    fireEvent.click(screen.getByTitle("rename this node"));
    await act(async () => {
      fireEvent.click(screen.getByText("save"));
    });

    expect(onRename).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("node name")).toBeNull();
  });

  it("rejects a blank name without calling rename", async () => {
    const onRename = vi.fn().mockResolvedValue(undefined);
    renderHeader(onRename);

    fireEvent.click(screen.getByTitle("rename this node"));
    fireEvent.change(screen.getByLabelText("node name"), {
      target: { value: "   " },
    });
    await act(async () => {
      fireEvent.click(screen.getByText("save"));
    });

    expect(onRename).not.toHaveBeenCalled();
    expect(screen.getByRole("alert").textContent).toBe(
      "name must not be empty",
    );
  });

  it("shows the server error and keeps the editor open", async () => {
    const onRename = vi.fn().mockRejectedValue(new Error("name is taken"));
    renderHeader(onRename);

    fireEvent.click(screen.getByTitle("rename this node"));
    fireEvent.change(screen.getByLabelText("node name"), {
      target: { value: "renamed" },
    });
    await act(async () => {
      fireEvent.click(screen.getByText("save"));
    });

    expect(onRename).toHaveBeenCalledWith("renamed");
    expect(screen.getByRole("alert").textContent).toBe("name is taken");
    expect(screen.getByLabelText("node name")).not.toBeNull();
  });

  it("cancels the edit without calling rename", () => {
    const onRename = vi.fn().mockResolvedValue(undefined);
    renderHeader(onRename);

    fireEvent.click(screen.getByTitle("rename this node"));
    fireEvent.click(screen.getByText("cancel"));

    expect(onRename).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("node name")).toBeNull();
  });
});

describe("Header backup button", () => {
  it("calls onBackup when the back up button is clicked", () => {
    const { onBackup } = renderHeader();

    fireEvent.click(screen.getByRole("button", { name: "back up" }));

    expect(onBackup).toHaveBeenCalledTimes(1);
  });

  it("disables the button and shows progress while a backup is running", () => {
    renderHeader(undefined, { backingUp: true });

    const button = screen.getByRole("button", { name: "backing up…" });
    expect((button as HTMLButtonElement).disabled).toBe(true);
  });

  it("disables the button without a node", () => {
    renderHeader(undefined, { node: null });

    expect(
      (screen.getByRole("button", { name: "back up" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("shows the backup error", () => {
    renderHeader(undefined, { backupError: "no data directory" });

    const alert = screen.getByRole("alert");
    expect(alert.textContent).toContain("no data directory");
  });

  it("shows no error when the last backup succeeded", () => {
    renderHeader(undefined, { backupError: null });

    expect(screen.queryByRole("alert")).toBeNull();
  });
});
