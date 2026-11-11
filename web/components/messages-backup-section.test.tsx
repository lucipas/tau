import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { MessagesExportDocument } from "@/lib/types";

import { MessagesBackupSection } from "./messages-backup-section";

describe("MessagesBackupSection", () => {
  const doc: MessagesExportDocument = {
    format: "tau-messages-export",
    version: 1,
    exported_at: 1000,
    messages: [{ id: "m-1", text: "hello" }],
  };

  function renderSection(
    overrides: {
      exporting?: boolean;
      importing?: boolean;
      onExport?: () => Promise<MessagesExportDocument>;
      onImport?: (doc: MessagesExportDocument) => Promise<{
        imported: number;
        present: number;
        skipped: number;
        errors: Array<{ id: string | null; error: string }>;
      }>;
    } = {},
  ) {
    const onExport = overrides.onExport ?? vi.fn(async () => doc);
    const onImport =
      overrides.onImport ??
      vi.fn(async () => ({
        imported: 2,
        present: 1,
        skipped: 0,
        errors: [],
      }));
    render(
      <MessagesBackupSection
        exporting={overrides.exporting ?? false}
        importing={overrides.importing ?? false}
        onExport={onExport}
        onImport={onImport}
      />,
    );
    return { onExport, onImport };
  }

  function pickFile(contents: string) {
    const file = new File([contents], "backup.json", {
      type: "application/json",
    });
    const input = document.querySelector(
      'input[type="file"]',
    ) as HTMLInputElement;
    fireEvent.change(input, { target: { files: [file] } });
  }

  it("exports the stored DMs and reports the message count", async () => {
    const { onExport } = renderSection();

    fireEvent.click(screen.getByText("export"));
    await waitFor(() => expect(onExport).toHaveBeenCalled());
    expect(await screen.findByText("exported 1 message")).toBeTruthy();
  });

  it("disables the export button while an export is running", () => {
    renderSection({ exporting: true });

    expect(screen.getByText("exporting…")).toBeTruthy();
    expect(
      (screen.getByText("exporting…") as HTMLButtonElement).disabled,
    ).toBe(true);
  });

  it("imports a picked export file and reports the counts", async () => {
    const { onImport } = renderSection();

    pickFile(JSON.stringify(doc));
    await waitFor(() => expect(onImport).toHaveBeenCalledWith(doc));
    expect(
      await screen.findByText("imported 2 · 1 already present · 0 skipped"),
    ).toBeTruthy();
  });

  it("lists per-entry errors from the import", async () => {
    const onImport = vi.fn(async () => ({
      imported: 0,
      present: 1,
      skipped: 1,
      errors: [{ id: "bad-1", error: "inbox record needs text" }],
    }));
    renderSection({ onImport });

    pickFile(JSON.stringify(doc));
    await waitFor(() => expect(onImport).toHaveBeenCalled());
    const line = await screen.findByText(/inbox record needs text/);
    expect(line.textContent).toContain("skipped");
  });

  it("rejects a file that is not a valid export document", async () => {
    const { onImport } = renderSection();

    pickFile("not json");
    expect(
      await screen.findByText("that file is not a valid export document"),
    ).toBeTruthy();
    expect(onImport).not.toHaveBeenCalled();
  });

  it("lets the same invalid file be picked again after a parse failure", async () => {
    const { onImport } = renderSection();
    const input = document.querySelector(
      'input[type="file"]',
    ) as HTMLInputElement;

    pickFile("not json");
    await screen.findByText("that file is not a valid export document");
    // The input is cleared even though parsing failed, so choosing
    // the same file again fires onChange instead of doing nothing.
    expect(input.value).toBe("");

    pickFile("not json");
    expect(
      await screen.findByText("that file is not a valid export document"),
    ).toBeTruthy();
    expect(onImport).not.toHaveBeenCalled();
  });
});
