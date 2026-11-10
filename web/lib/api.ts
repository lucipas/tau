import type { ApiErrorBody } from "./types";

function resolveApiBase(): string {
  const configured = process.env.NEXT_PUBLIC_API_BASE;
  if (configured !== undefined) {
    return configured.replace(/\/$/, "");
  }
  if (typeof window === "undefined") {
    return "";
  }
  // `next dev` runs on :3000 while the tau bridge listens on :8787.
  const { protocol, hostname, port } = window.location;
  if ((port === "3000" || port === "3001") && isLocalHost(hostname)) {
    return `${protocol}//${hostname}:8787`;
  }
  return "";
}

function isLocalHost(hostname: string): boolean {
  return (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "::1" ||
    hostname.endsWith(".localhost")
  );
}

export const API_BASE = resolveApiBase();

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function parseResponse<T>(response: Response): Promise<T> {
  const raw = await response.text();
  let payload: unknown = null;
  if (raw) {
    try {
      payload = JSON.parse(raw);
    } catch {
      payload = null;
    }
  }
  if (!response.ok) {
    const body = payload as ApiErrorBody | null;
    throw new ApiError(
      body?.error?.message ?? `request failed (${response.status})`,
      response.status,
    );
  }
  return payload as T;
}

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  return parseResponse<T>(response);
}

export async function apiPost<T>(
  path: string,
  body: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
  });
  return parseResponse<T>(response);
}

export function eventsUrl(since = 0): string {
  return `${API_BASE}/api/events?since=${since}`;
}

export interface DownloadResult {
  blob: Blob;
  filename: string;
}

function filenameFromDisposition(header: string | null): string {
  if (!header) return "tau-backup.tar.gz";
  const match = /filename\*?=(?:UTF-8''|")?([^";]+)"?/.exec(header);
  if (!match) return "tau-backup.tar.gz";
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return match[1];
  }
}

export async function apiDownload(path: string): Promise<DownloadResult> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!response.ok) {
    // A failed download carries the usual JSON error body, not a blob.
    const raw = await response.text();
    let message = `request failed (${response.status})`;
    if (raw) {
      try {
        const body = JSON.parse(raw) as ApiErrorBody | null;
        if (body?.error?.message) message = body.error.message;
      } catch {
        // Non-JSON error body: keep the status fallback.
      }
    }
    throw new ApiError(message, response.status);
  }
  const blob = await response.blob();
  return {
    blob,
    filename: filenameFromDisposition(
      response.headers.get("Content-Disposition"),
    ),
  };
}
