// BeaconControl.tsx — share live location with the open conversation.
//
// A live beacon sends this node's position to one DM peer or group as a
// series of location updates until it expires. Starting one needs a first
// position: the browser's geolocation when available, or a typed
// latitude/longitude when it is not. Once started, the shell-level feed
// (useBeaconFeed, mounted in the Shell) keeps the beacon's position file
// fresh from watchPosition, so sharing survives leaving the chat tab;
// without geolocation the beacon simply repeats its start position,
// like a CLI beacon with no position file.

"use client";

import { useState } from "react";
import { startBeacon, stopBeacon } from "@/lib/api";
import { formatCountdown } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import type { Beacon } from "@/lib/types";

export interface BeaconTarget {
  kind: "dm" | "group";
  key: string;
  name: string;
}

interface BeaconControlProps {
  target: BeaconTarget;
  beacons: Beacon[];
  onRefresh: () => Promise<void> | void;
  onError: (message: string) => void;
}

const DURATIONS = [
  { label: "15 minutes", seconds: 900 },
  { label: "1 hour", seconds: 3600 },
  { label: "4 hours", seconds: 14400 },
];

/** Validate a typed latitude/longitude, like the console's pin mode. */
export function validateBeaconPosition(lat: string, lon: string): string | null {
  const parsedLat = Number(lat);
  const parsedLon = Number(lon);
  if (lat.trim() === "" || lon.trim() === "" || !Number.isFinite(parsedLat) || !Number.isFinite(parsedLon)) {
    return "Enter a latitude and a longitude.";
  }
  if (parsedLat < -90 || parsedLat > 90) {
    return "Latitude must be between -90 and 90.";
  }
  if (parsedLon < -180 || parsedLon > 180) {
    return "Longitude must be between -180 and 180.";
  }
  return null;
}

function activeBeaconFor(beacons: Beacon[], target: BeaconTarget): Beacon | undefined {
  const kind = target.kind === "group" ? "group" : "dm";
  return beacons.find((b) => b.active && b.target === target.key && b.target_kind === kind);
}

function geolocationAvailable(): boolean {
  return typeof navigator !== "undefined" && !!navigator.geolocation;
}

function currentPosition(): Promise<GeolocationPosition> {
  return new Promise((resolve, reject) => {
    navigator.geolocation.getCurrentPosition(resolve, reject, {
      enableHighAccuracy: false,
      maximumAge: 30000,
      timeout: 10000,
    });
  });
}

export default function BeaconControl({ target, beacons, onRefresh, onError }: BeaconControlProps) {
  const now = useNow(1000);
  const [showForm, setShowForm] = useState(false);
  const [durationSeconds, setDurationSeconds] = useState(3600);
  const [coarse, setCoarse] = useState(false);
  const [lat, setLat] = useState("");
  const [lon, setLon] = useState("");
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [locating, setLocating] = useState(false);

  const active = activeBeaconFor(beacons, target);
  const geo = geolocationAvailable();

  async function start(latitude: number, longitude: number, place?: string) {
    setBusy(true);
    try {
      await startBeacon(
        target.kind === "group" ? { group: target.key } : { peer: target.key },
        {
          lat: latitude,
          lon: longitude,
          ...(place ? { name: place } : {}),
          durationSeconds,
          coarse,
        },
      );
      setShowForm(false);
      setError(null);
      await onRefresh();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      onError(`Live location failed: ${message}`);
    } finally {
      setBusy(false);
    }
  }

  async function startWithGeolocation() {
    setLocating(true);
    setError(null);
    try {
      const pos = await currentPosition();
      await start(pos.coords.latitude, pos.coords.longitude);
    } catch {
      setError("Could not read your location — type a position instead.");
    } finally {
      setLocating(false);
    }
  }

  function startWithTyped() {
    const problem = validateBeaconPosition(lat, lon);
    if (problem) {
      setError(problem);
      return;
    }
    void start(Number(lat), Number(lon), name.trim() || undefined);
  }

  async function stop() {
    if (!active) return;
    setBusy(true);
    try {
      await stopBeacon(active.id);
      setError(null);
      await onRefresh();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      onError(`Stop failed: ${message}`);
    } finally {
      setBusy(false);
    }
  }

  if (active) {
    return (
      <div className="flex items-center justify-between gap-3 border-b border-zinc-800 bg-cyan-950/40 px-3 py-2 text-sm">
        <p className="min-w-0 text-zinc-200">
          📍 Sharing live location with {target.name} · expires {formatCountdown(active.expires_at, now)}
          {active.coarse ? " · coarse (~1 km)" : ""}
        </p>
        <button
          type="button"
          disabled={busy}
          onClick={() => void stop()}
          className="shrink-0 rounded-full border border-red-800 px-3 py-1 text-xs font-semibold text-red-300 hover:bg-red-950 disabled:opacity-40"
        >
          Stop sharing
        </button>
      </div>
    );
  }

  if (!showForm) {
    return (
      <div className="border-b border-zinc-800 px-3 py-2">
        <button
          type="button"
          onClick={() => setShowForm(true)}
          className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-800"
        >
          📍 Share live location
        </button>
      </div>
    );
  }

  return (
    <div className="border-b border-zinc-800 px-3 py-3">
      <h3 className="text-sm font-bold text-zinc-100">Share live location with {target.name}</h3>
      {error && (
        <p role="alert" className="mt-2 text-xs text-red-300">
          {error}
        </p>
      )}
      <div className="mt-2 flex flex-col gap-2">
        <label className="text-xs font-semibold text-zinc-400" htmlFor="beacon-duration">
          Duration
        </label>
        <select
          id="beacon-duration"
          value={durationSeconds}
          onChange={(e) => setDurationSeconds(Number(e.target.value))}
          className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500"
        >
          {DURATIONS.map((d) => (
            <option key={d.seconds} value={d.seconds}>
              {d.label}
            </option>
          ))}
        </select>
        <label className="flex items-center gap-2 text-sm text-zinc-200">
          <input type="checkbox" checked={coarse} onChange={(e) => setCoarse(e.target.checked)} />
          Coarse precision (~1 km, no place name)
        </label>
        {geo ? (
          <button
            type="button"
            disabled={busy || locating}
            onClick={() => void startWithGeolocation()}
            className="rounded-full bg-cyan-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-cyan-500 disabled:opacity-40"
          >
            {locating ? "Locating…" : "Use my location"}
          </button>
        ) : (
          <p className="text-xs text-zinc-500">
            Geolocation is unavailable here — type a position instead. The beacon repeats it until it expires.
          </p>
        )}
        <div className="flex gap-2">
          <label className="sr-only" htmlFor="beacon-lat">
            Latitude
          </label>
          <input
            id="beacon-lat"
            value={lat}
            onChange={(e) => setLat(e.target.value)}
            placeholder="Latitude"
            inputMode="decimal"
            className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          <label className="sr-only" htmlFor="beacon-lon">
            Longitude
          </label>
          <input
            id="beacon-lon"
            value={lon}
            onChange={(e) => setLon(e.target.value)}
            placeholder="Longitude"
            inputMode="decimal"
            className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
        </div>
        <label className="sr-only" htmlFor="beacon-name">
          Place name (optional)
        </label>
        <input
          id="beacon-name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Place name (optional)"
          className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
        />
        <div className="flex gap-2">
          <button
            type="button"
            disabled={busy || locating}
            onClick={startWithTyped}
            className="rounded-full bg-cyan-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-cyan-500 disabled:opacity-40"
          >
            Start sharing
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => {
              setShowForm(false);
              setError(null);
            }}
            className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-800 disabled:opacity-40"
          >
            Cancel
          </button>
        </div>
        <p className="text-xs text-zinc-500">
          Updates go out every few minutes until the beacon expires. Your position stays in the conversation — it is never
          sent anywhere else.
        </p>
      </div>
    </div>
  );
}
