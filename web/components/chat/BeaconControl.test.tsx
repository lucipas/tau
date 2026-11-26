import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import BeaconControl, { validateBeaconPosition } from "./BeaconControl";
import { startBeacon, stopBeacon } from "@/lib/api";
import type { Beacon } from "@/lib/types";

vi.mock("@/lib/api", () => ({
  startBeacon: vi.fn(async () => ({ beacon: { id: "aabbccdd" }, update: {} })),
  stopBeacon: vi.fn(async () => ({ beacon: { id: "aabbccdd" } })),
}));

const mockedStart = vi.mocked(startBeacon);
const mockedStop = vi.mocked(stopBeacon);

const TARGET = { kind: "dm" as const, key: "ab".repeat(16), name: "alice" };

function makeBeacon(over: Partial<Beacon> = {}): Beacon {
  return {
    id: "aabbccdd",
    target: "ab".repeat(16),
    target_kind: "dm",
    target_name: "alice",
    started_at: 1700000000,
    expires_at: Date.now() / 1000 + 1860,
    interval: 300,
    coarse: false,
    location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
    next_update_at: 1700000300,
    updates_sent: 1,
    stopped: false,
    active: true,
    expires_in_seconds: 1860,
    ...over,
  };
}

afterEach(() => {
  vi.clearAllMocks();
  vi.unstubAllGlobals();
  const nav = navigator as unknown as Record<string, unknown>;
  if ("geolocation" in nav) delete nav.geolocation;
});

describe("validateBeaconPosition", () => {
  it("rejects missing and out-of-range coordinates", () => {
    expect(validateBeaconPosition("", "-83.88")).toMatch(/latitude and a longitude/);
    expect(validateBeaconPosition("91", "-83.88")).toMatch(/Latitude must be between/);
    expect(validateBeaconPosition("34.05", "-181")).toMatch(/Longitude must be between/);
    expect(validateBeaconPosition("34.05", "-83.88")).toBeNull();
  });
});

describe("BeaconControl", () => {
  it("offers to share live location when nothing is active", () => {
    render(
      <BeaconControl target={TARGET} beacons={[]} onRefresh={() => undefined} onError={() => undefined} />,
    );
    expect(screen.getByRole("button", { name: /Share live location/ })).toBeTruthy();
    expect(screen.queryByText(/Stop sharing/)).toBeNull();
  });

  it("starts a beacon from a typed position", async () => {
    const onRefresh = vi.fn();
    render(
      <BeaconControl target={TARGET} beacons={[]} onRefresh={onRefresh} onError={() => undefined} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Share live location/ }));
    // jsdom has no geolocation, so the typed fallback is offered.
    expect(screen.getByText(/Geolocation is unavailable/)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Latitude"), { target: { value: "34.05" } });
    fireEvent.change(screen.getByLabelText("Longitude"), { target: { value: "-83.88" } });
    fireEvent.change(screen.getByLabelText("Place name (optional)"), {
      target: { value: "Rock Springs Park" },
    });
    fireEvent.click(screen.getByText("Start sharing"));
    await waitFor(() =>
      expect(mockedStart).toHaveBeenCalledWith(
        { peer: "ab".repeat(16) },
        {
          lat: 34.05,
          lon: -83.88,
          name: "Rock Springs Park",
          durationSeconds: 3600,
          coarse: false,
        },
      ),
    );
    expect(onRefresh).toHaveBeenCalled();
  });

  it("refuses a typed position with a bad latitude", () => {
    render(
      <BeaconControl target={TARGET} beacons={[]} onRefresh={() => undefined} onError={() => undefined} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Share live location/ }));
    fireEvent.change(screen.getByLabelText("Latitude"), { target: { value: "91" } });
    fireEvent.change(screen.getByLabelText("Longitude"), { target: { value: "-83.88" } });
    fireEvent.click(screen.getByText("Start sharing"));
    expect(screen.getByRole("alert").textContent).toMatch(/Latitude must be between/);
    expect(mockedStart).not.toHaveBeenCalled();
  });

  it("starts from the browser's geolocation when available", async () => {
    const getCurrentPosition = vi.fn((success: PositionCallback) =>
      success({
        coords: {
          latitude: 34.05,
          longitude: -83.88,
          accuracy: 10,
          altitude: null,
          altitudeAccuracy: null,
          heading: null,
          speed: null,
        },
        timestamp: Date.now(),
      } as GeolocationPosition),
    );
    Object.defineProperty(navigator, "geolocation", {
      value: { getCurrentPosition },
      configurable: true,
    });
    render(
      <BeaconControl target={TARGET} beacons={[]} onRefresh={() => undefined} onError={() => undefined} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Share live location/ }));
    fireEvent.click(screen.getByText("Use my location"));
    await waitFor(() =>
      expect(mockedStart).toHaveBeenCalledWith(
        { peer: "ab".repeat(16) },
        expect.objectContaining({ lat: 34.05, lon: -83.88, durationSeconds: 3600 }),
      ),
    );
    expect(getCurrentPosition).toHaveBeenCalled();
  });

  it("shows the active beacon with a countdown and stops it", async () => {
    const onRefresh = vi.fn();
    render(
      <BeaconControl
        target={TARGET}
        beacons={[makeBeacon()]}
        onRefresh={onRefresh}
        onError={() => undefined}
      />,
    );
    expect(screen.getByText(/Sharing live location with alice/)).toBeTruthy();
    expect(screen.getByText(/expires in 3[01]m/)).toBeTruthy();
    fireEvent.click(screen.getByText("Stop sharing"));
    await waitFor(() => expect(mockedStop).toHaveBeenCalledWith("aabbccdd"));
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });

  it("starts a broadcast beacon with the list name", async () => {
    const onRefresh = vi.fn();
    render(
      <BeaconControl
        target={{ kind: "broadcast", key: "family", name: "family" }}
        beacons={[]}
        onRefresh={onRefresh}
        onError={() => undefined}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Share live location/ }));
    expect(screen.getByText(/private message/)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Latitude"), { target: { value: "34.05" } });
    fireEvent.change(screen.getByLabelText("Longitude"), { target: { value: "-83.88" } });
    fireEvent.click(screen.getByText("Start sharing"));
    await waitFor(() =>
      expect(mockedStart).toHaveBeenCalledWith(
        { broadcast: "family" },
        expect.objectContaining({ lat: 34.05, lon: -83.88, durationSeconds: 3600 }),
      ),
    );
    expect(onRefresh).toHaveBeenCalled();
  });

  it("shows the active broadcast beacon and stops it", async () => {
    const onRefresh = vi.fn();
    render(
      <BeaconControl
        target={{ kind: "broadcast", key: "family", name: "family" }}
        beacons={[
          makeBeacon({
            target: "family",
            target_kind: "broadcast",
            target_name: "family",
          }),
        ]}
        onRefresh={onRefresh}
        onError={() => undefined}
      />,
    );
    expect(screen.getByText(/Sharing live location with family/)).toBeTruthy();
    fireEvent.click(screen.getByText("Stop sharing"));
    await waitFor(() => expect(mockedStop).toHaveBeenCalledWith("aabbccdd"));
    await waitFor(() => expect(onRefresh).toHaveBeenCalled());
  });

  it("ignores beacons for other conversations", () => {
    render(
      <BeaconControl
        target={TARGET}
        beacons={[makeBeacon({ id: "ffffffff", target: "cd".repeat(16), target_kind: "dm" })]}
        onRefresh={() => undefined}
        onError={() => undefined}
      />,
    );
    expect(screen.getByRole("button", { name: /Share live location/ })).toBeTruthy();
  });
});
