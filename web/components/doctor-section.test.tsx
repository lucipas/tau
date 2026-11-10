import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { DoctorResponse } from "@/lib/types";

import { DoctorSection } from "./doctor-section";

function makeReport(overrides: Partial<DoctorResponse> = {}): DoctorResponse {
  return {
    ok: true,
    checks: [
      { name: "identity", status: "ok", detail: "loaded" },
      { name: "peers", status: "ok", detail: "1 peer(s)" },
    ],
    ...overrides,
  };
}

describe("DoctorSection", () => {
  it("starts idle and runs the check on demand", async () => {
    const onRun = vi.fn(async () => makeReport());

    render(<DoctorSection onRun={onRun} />);

    expect(
      screen.getByRole("button", { name: "run health check" }),
    ).toBeTruthy();
    expect(screen.queryByText("identity")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "run health check" }));
    await waitFor(() => expect(onRun).toHaveBeenCalledTimes(1));
    expect(await screen.findByText("identity")).toBeTruthy();
    expect(screen.getByText("healthy")).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "run again" }),
    ).toBeTruthy();
  });

  it("summarises failed checks and keeps their details visible", async () => {
    const onRun = vi.fn(async () =>
      makeReport({
        ok: false,
        checks: [
          {
            name: "identity",
            status: "fail",
            detail: "no identity — run `tau init` first",
          },
          { name: "peers", status: "ok", detail: "no peers yet" },
        ],
      }),
    );

    render(<DoctorSection onRun={onRun} />);

    fireEvent.click(screen.getByRole("button", { name: "run health check" }));
    expect(await screen.findByText("1 check failed")).toBeTruthy();
    expect(
      screen.getByText("no identity — run `tau init` first"),
    ).toBeTruthy();
    expect(screen.getByText("fail")).toBeTruthy();
  });

  it("shows the error when the check fails", async () => {
    const onRun = vi.fn(async () => {
      throw new Error("bridge unreachable");
    });

    render(<DoctorSection onRun={onRun} />);

    fireEvent.click(screen.getByRole("button", { name: "run health check" }));
    expect(await screen.findByText("bridge unreachable")).toBeTruthy();
  });
});
