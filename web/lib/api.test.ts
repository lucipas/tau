import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, apiDownload } from "./api";

function mockFetch(response: Response) {
  const fetchMock = vi.fn().mockResolvedValue(response);
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("apiDownload", () => {
  it("returns the blob and the filename from Content-Disposition", async () => {
    const fetchMock = mockFetch(
      new Response("archive-bytes", {
        status: 200,
        headers: {
          "Content-Type": "application/gzip",
          "Content-Disposition":
            'attachment; filename="tau-backup-alice-20261110-120000.tar.gz"',
        },
      }),
    );

    const result = await apiDownload("/api/backup");

    expect(await result.blob.text()).toBe("archive-bytes");
    expect(result.filename).toBe("tau-backup-alice-20261110-120000.tar.gz");
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/backup"),
      expect.objectContaining({ cache: "no-store" }),
    );
  });

  it("falls back to a default filename without Content-Disposition", async () => {
    mockFetch(new Response("x", { status: 200 }));

    const result = await apiDownload("/api/backup");

    expect(result.filename).toBe("tau-backup.tar.gz");
  });

  it("throws an ApiError carrying the server's JSON message", async () => {
    mockFetch(
      new Response(
        JSON.stringify({ error: { message: "no data directory" } }),
        {
          status: 503,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );

    let caught: unknown;
    try {
      await apiDownload("/api/backup");
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect((caught as ApiError).message).toBe("no data directory");
    expect((caught as ApiError).status).toBe(503);
  });

  it("falls back to the status when the error body is not JSON", async () => {
    mockFetch(new Response("gateway exploded", { status: 502 }));

    let caught: unknown;
    try {
      await apiDownload("/api/backup");
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect((caught as ApiError).message).toBe("request failed (502)");
  });
});
