import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  BundleExportResponse,
  BundleImportResponse,
} from "@/lib/types";

import { BundleSection } from "./bundle-section";

// The server's serialised bundle text, exactly as an export returns it.
const BUNDLE_TEXT = `{
  "format": "tau-bundle",
  "version": 1,
  "manifest": {
    "created_at": 1700000000.0,
    "exporter": "${"ab".repeat(16)}",
    "exporter_key": "${"cd".repeat(32)}",
    "mail": 0,
    "tan": 0
  },
  "mail": [],
  "tan": null,
  "sig": "c2ln"
}
`;

function makeExportResponse(): BundleExportResponse {
  return {
    ok: true,
    bundle: BUNDLE_TEXT,
    summary: { mail: 2, tan: 3, skipped: [], bytes: 2048 },
  };
}

function makeImportResponse(
  overrides: Partial<BundleImportResponse["import"]> = {},
): BundleImportResponse {
  return {
    ok: true,
    import: {
      imported: 1,
      present: 0,
      skipped: 0,
      tan: null,
      errors: [],
      ...overrides,
    },
  };
}

function stubDownload() {
  const blobParts: unknown[] = [];
  class FakeBlob {
    constructor(parts: unknown[]) {
      blobParts.push(...parts);
    }
  }
  vi.stubGlobal("Blob", FakeBlob);
  vi.stubGlobal("URL", {
    ...URL,
    createObjectURL: vi.fn(() => "blob:fake"),
    revokeObjectURL: vi.fn(),
  });
  const click = vi.fn();
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(click);
  return { blobParts, click };
}

function unstubDownload() {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
}

describe("BundleSection", () => {
  it("exports a bundle, downloads the server text verbatim and shows the summary", async () => {
    const { blobParts, click } = stubDownload();
    const onExport = vi.fn(async () => makeExportResponse());

    render(<BundleSection onExport={onExport} onImport={vi.fn()} />);

    fireEvent.click(screen.getByText("download bundle"));

    await waitFor(() => {
      expect(onExport).toHaveBeenCalledWith(false);
      expect(click).toHaveBeenCalledTimes(1);
      expect(
        screen.getByText(/2 sealed messages · 3 Tan posts · 2\.0 KiB/),
      ).toBeTruthy();
    });
    // The downloaded file is the server's text as-is: re-stringifying
    // the document could rewrite a number the signature covers.
    expect(blobParts).toEqual([BUNDLE_TEXT]);
    unstubDownload();
  });

  it("passes the include-Tan choice through to the export", async () => {
    stubDownload();
    const onExport = vi.fn(async () => makeExportResponse());

    render(<BundleSection onExport={onExport} onImport={vi.fn()} />);

    fireEvent.click(screen.getByLabelText("include recent Tan posts"));
    fireEvent.click(screen.getByText("download bundle"));

    await waitFor(() => {
      expect(onExport).toHaveBeenCalledWith(true);
    });
    unstubDownload();
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

  it("imports a chosen bundle file verbatim and shows the merge report", async () => {
    const onImport = vi.fn(async () => makeImportResponse());

    render(<BundleSection onExport={vi.fn()} onImport={onImport} />);

    const input = document.getElementById("bundle-file") as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File([BUNDLE_TEXT], "bundle.json", {
        type: "application/json",
      })] },
    });

    await waitFor(() => {
      expect(onImport).toHaveBeenCalledTimes(1);
      expect(
        screen.getByText(/imported 1 · already held 0 · skipped 0/),
      ).toBeTruthy();
    });
    // The file's text reaches the server byte-for-byte: the integral
    // float in the signed manifest must survive the trip.
    expect(onImport).toHaveBeenCalledWith(BUNDLE_TEXT);
  });

  it("shows skipped Tan posts with their reasons", async () => {
    const onImport = vi.fn(async () =>
      makeImportResponse({
        imported: 0,
        present: 0,
        skipped: 1,
        tan: {
          imported: 0,
          present: 0,
          skipped: 1,
          errors: [
            {
              hash: "abc123",
              error: "entry denied by this node's Tan ruleset",
            },
          ],
        },
      }),
    );

    render(<BundleSection onExport={vi.fn()} onImport={onImport} />);

    const input = document.getElementById("bundle-file") as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File([BUNDLE_TEXT], "bundle.json", {
        type: "application/json",
      })] },
    });

    await waitFor(() => {
      expect(
        screen.getByText(
          "Tan skipped abc123: entry denied by this node's Tan ruleset",
        ),
      ).toBeTruthy();
    });
  });

  it("shows an import failure from the bridge", async () => {
    const onImport = vi.fn(async () => {
      throw new Error("invalid bundle: bundle is not valid JSON");
    });

    render(<BundleSection onExport={vi.fn()} onImport={onImport} />);

    const input = document.getElementById("bundle-file") as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File(["{not json"], "bundle.json", {
        type: "application/json",
      })] },
    });

    await waitFor(() => {
      expect(
        screen.getByText("invalid bundle: bundle is not valid JSON"),
      ).toBeTruthy();
    });
    // The file's text still went to the server: the bridge, not the
    // console, decides what counts as a bundle.
    expect(onImport).toHaveBeenCalledWith("{not json");
  });
});
