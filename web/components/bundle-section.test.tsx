import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  BundleDocument,
  BundleExportResponse,
  BundleImportResponse,
} from "@/lib/types";

import { BundleSection } from "./bundle-section";

function makeBundle(overrides: Partial<BundleDocument> = {}): BundleDocument {
  return {
    format: "tau-bundle",
    version: 1,
    manifest: { mail: 1, tan: 0 },
    mail: [],
    tan: null,
    sig: "c2ln",
    ...overrides,
  };
}

function makeExportResponse(): BundleExportResponse {
  return {
    ok: true,
    bundle: makeBundle(),
    summary: { mail: 2, tan: 3, skipped: [], bytes: 2048 },
  };
}

function makeImportResponse(): BundleImportResponse {
  return {
    ok: true,
    import: {
      imported: 1,
      present: 0,
      skipped: 0,
      tan: null,
      errors: [],
    },
  };
}

describe("BundleSection", () => {
  it("exports a bundle, downloads it and shows the summary", async () => {
    const createObjectURL = vi.fn(() => "blob:fake");
    const revokeObjectURL = vi.fn();
    vi.stubGlobal("URL", { ...URL, createObjectURL, revokeObjectURL });
    const click = vi.fn();
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(click);
    const onExport = vi.fn(async () => makeExportResponse());

    render(<BundleSection onExport={onExport} onImport={vi.fn()} />);

    fireEvent.click(screen.getByText("download bundle"));

    await waitFor(() => {
      expect(onExport).toHaveBeenCalledWith(false);
      expect(createObjectURL).toHaveBeenCalledTimes(1);
      expect(click).toHaveBeenCalledTimes(1);
      expect(
        screen.getByText(/2 sealed messages · 3 Tan posts · 2\.0 KiB/),
      ).toBeTruthy();
    });
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:fake");
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("passes the include-Tan choice through to the export", async () => {
    vi.stubGlobal("URL", {
      ...URL,
      createObjectURL: vi.fn(() => "blob:fake"),
      revokeObjectURL: vi.fn(),
    });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    const onExport = vi.fn(async () => makeExportResponse());

    render(<BundleSection onExport={onExport} onImport={vi.fn()} />);

    fireEvent.click(screen.getByLabelText("include recent Tan posts"));
    fireEvent.click(screen.getByText("download bundle"));

    await waitFor(() => {
      expect(onExport).toHaveBeenCalledWith(true);
    });
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("shows the export error instead of a summary", async () => {
    const onExport = vi.fn(async () => {
      throw new Error("this node has no identity yet");
    });

    render(<BundleSection onExport={onExport} onImport={vi.fn()} />);

    fireEvent.click(screen.getByText("download bundle"));

    await waitFor(() => {
      expect(
        screen.getByText("this node has no identity yet"),
      ).toBeTruthy();
    });
    expect(screen.queryByText(/sealed messages/)).toBeNull();
  });

  it("imports a chosen bundle file and shows the merge report", async () => {
    const onImport = vi.fn(async () => makeImportResponse());
    const file = new File([JSON.stringify(makeBundle())], "bundle.json", {
      type: "application/json",
    });

    render(<BundleSection onExport={vi.fn()} onImport={onImport} />);

    const input = document.getElementById("bundle-file") as HTMLInputElement;
    fireEvent.change(input, { target: { files: [file] } });

    await waitFor(() => {
      expect(onImport).toHaveBeenCalledTimes(1);
      expect(
        screen.getByText(/imported 1 · already held 0 · skipped 0/),
      ).toBeTruthy();
    });
    expect(onImport).toHaveBeenCalledWith(
      expect.objectContaining({ format: "tau-bundle" }),
    );
  });

  it("reports a file that is not valid JSON instead of importing", async () => {
    const onImport = vi.fn();
    const file = new File(["{not json"], "bundle.json", {
      type: "application/json",
    });

    render(<BundleSection onExport={vi.fn()} onImport={onImport} />);

    const input = document.getElementById("bundle-file") as HTMLInputElement;
    fireEvent.change(input, { target: { files: [file] } });

    await waitFor(() => {
      expect(screen.getByText("that file is not valid JSON")).toBeTruthy();
    });
    expect(onImport).not.toHaveBeenCalled();
  });

  it("shows an import failure from the bridge", async () => {
    const onImport = vi.fn(async () => {
      throw new Error("invalid bundle: bundle signature does not verify");
    });
    const file = new File([JSON.stringify(makeBundle())], "bundle.json", {
      type: "application/json",
    });

    render(<BundleSection onExport={vi.fn()} onImport={onImport} />);

    const input = document.getElementById("bundle-file") as HTMLInputElement;
    fireEvent.change(input, { target: { files: [file] } });

    await waitFor(() => {
      expect(
        screen.getByText(
          "invalid bundle: bundle signature does not verify",
        ),
      ).toBeTruthy();
    });
  });
});
