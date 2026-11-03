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
