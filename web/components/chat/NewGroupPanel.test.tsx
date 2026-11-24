// NewGroupPanel.test.tsx — the console group-create form: validation,
// the create call with picked peers, and API errors shown inline.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import NewGroupPanel from "./NewGroupPanel";
import { makePeer } from "../test-utils";
import type { Group } from "@/lib/types";

const ALICE = "a".repeat(32);
const BOB = "b".repeat(32);

const CREW: Group = {
  id: "c".repeat(32),
  name: "crew",
  members: [
    { hash: ALICE, name: "Alice" },
    { hash: BOB, name: "Bob" },
  ],
  created_at: 1700000000,
  topic: "",
  created_by_me: true,
  can_leave: true,
  ignored: false,
};

function renderPanel(
  overrides: Partial<Parameters<typeof NewGroupPanel>[0]> = {},
) {
  const props = {
    peers: [
      makePeer({ destination: ALICE, name: "Alice" }),
      makePeer({ destination: BOB, name: "Bob" }),
    ],
    onCreate: vi.fn(async (): Promise<Group> => CREW),
    ...overrides,
  };
  const { rerender } = render(<NewGroupPanel {...props} />);
  return {
    ...props,
    rerender: (next: Partial<Parameters<typeof NewGroupPanel>[0]>) =>
      rerender(<NewGroupPanel {...props} {...next} />),
  };
}

function createNamedGroup(name: string, member: string) {
  fireEvent.change(screen.getByLabelText("Group name"), {
    target: { value: name },
  });
  fireEvent.click(screen.getByLabelText(member));
  fireEvent.click(screen.getByText("Create group"));
}

describe("NewGroupPanel", () => {
  it("creates a group from the form with picked peers", async () => {
    const props = renderPanel();
    createNamedGroup("crew", "Alice");
    await vi.waitFor(() => {
      expect(props.onCreate).toHaveBeenCalledWith("crew", [ALICE]);
    });
    // The form clears once the create lands, ready for the next group.
    await vi.waitFor(() => {
      expect((screen.getByLabelText("Group name") as HTMLInputElement).value).toBe("");
    });
  });

  it("requires a name before creating", async () => {
    const props = renderPanel();
    fireEvent.click(screen.getByLabelText("Alice"));
    fireEvent.click(screen.getByText("Create group"));
    expect(screen.getByText("Give the group a name.")).toBeTruthy();
    expect(props.onCreate).not.toHaveBeenCalled();
  });

  it("requires at least one picked peer", async () => {
    const props = renderPanel();
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "crew" },
    });
    fireEvent.click(screen.getByText("Create group"));
    expect(screen.getByText("Pick at least one peer.")).toBeTruthy();
    expect(props.onCreate).not.toHaveBeenCalled();
  });

  it("shows an API error and keeps the form for a retry", async () => {
    const props = renderPanel({
      onCreate: vi.fn(async (): Promise<Group> => {
        throw new Error("unknown peer: " + BOB);
      }),
    });
    createNamedGroup("crew", "Bob");
    await vi.waitFor(() => {
      expect(screen.getByText("unknown peer: " + BOB)).toBeTruthy();
    });
    expect((screen.getByLabelText("Group name") as HTMLInputElement).value).toBe("crew");
    expect(props.onCreate).toHaveBeenCalledTimes(1);
  });

  it("drops a picked peer who disappeared before creating", async () => {
    const panel = renderPanel();
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "crew" },
    });
    fireEvent.click(screen.getByLabelText("Alice"));
    fireEvent.click(screen.getByLabelText("Bob"));
    // Bob drops out of the registry while the form is open: his hash
    // must not be submitted, and there is no checkbox left to uncheck.
    panel.rerender({
      peers: [makePeer({ destination: ALICE, name: "Alice" })],
    });
    fireEvent.click(screen.getByText("Create group"));
    await vi.waitFor(() => {
      expect(panel.onCreate).toHaveBeenCalledWith("crew", [ALICE]);
    });
  });

  it("creates a secret group when the secret box is checked", async () => {
    const props = renderPanel();
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "crew" },
    });
    fireEvent.click(screen.getByLabelText("Alice"));
    fireEvent.click(screen.getByLabelText(/Secret group/));
    fireEvent.click(screen.getByText("Create group"));
    await vi.waitFor(() => {
      expect(props.onCreate).toHaveBeenCalledWith("crew", [ALICE], {
        secret: true,
      });
    });
  });

  it("says so when no peer has announced yet", () => {
    renderPanel({ peers: [] });
    expect(screen.getByText(/No known peers yet/)).toBeTruthy();
  });
});
