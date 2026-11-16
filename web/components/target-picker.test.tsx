import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { TargetPicker } from "./target-picker";

const peers = [
  {
    destination: "a".repeat(64),
    name: "alice",
    last_seen: 1000,
    age: 0,
    hops: 1,
    protocol: 1,
  },
];
const groups = [
  { id: "g".repeat(32), name: "crew", members: [], created_at: 1000 },
];

describe("TargetPicker", () => {
  it("calls back with the picked peer's destination", () => {
    const onSelectPeer = vi.fn();
    render(
      <TargetPicker
        peers={peers}
        groups={groups}
        onSelectPeer={onSelectPeer}
        onSelectGroup={vi.fn()}
        verb="Forward to"
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "alice" }));
    expect(onSelectPeer).toHaveBeenCalledWith(peers[0].destination);
  });

  it("calls back with the picked group's id", () => {
    const onSelectGroup = vi.fn();
    render(
      <TargetPicker
        peers={peers}
        groups={groups}
        onSelectPeer={vi.fn()}
        onSelectGroup={onSelectGroup}
        verb="Share with"
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "#crew" }));
    expect(onSelectGroup).toHaveBeenCalledWith(groups[0].id);
  });

  it("says so when there are no known peers or groups", () => {
    render(
      <TargetPicker
        peers={[]}
        groups={[]}
        onSelectPeer={vi.fn()}
        onSelectGroup={vi.fn()}
        verb="Forward to"
      />,
    );
    expect(screen.getByText("no known peers or groups yet")).toBeTruthy();
  });

  it("disables the chips while a send is pending", () => {
    render(
      <TargetPicker
        peers={peers}
        groups={groups}
        pending
        onSelectPeer={vi.fn()}
        onSelectGroup={vi.fn()}
        verb="Forward to"
      />,
    );
    expect(
      (screen.getByRole("button", { name: "alice" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "#crew" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });
});
