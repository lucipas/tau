import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import RemindersPanel from "./RemindersPanel";
import type { Reminder } from "@/lib/types";

const now = Date.now() / 1000;
const reminders: Reminder[] = [
  {
    id: "aa".repeat(8),
    message_id: "wire-1",
    kind: "peer",
    target: "ab".repeat(16),
    target_name: "alice",
    text: "hello alice\nsecond line",
    sender: "alice",
    due_at: now + 3660,
    created_at: now,
  },
  {
    id: "bb".repeat(8),
    message_id: "wire-2",
    kind: "group",
    target: "cd".repeat(16),
    target_name: "crew",
    text: "hello crew",
    sender: "bob",
    due_at: now + 3 * 3660,
    created_at: now,
  },
];

describe("RemindersPanel", () => {
  it("renders nothing when there are no reminders", () => {
    render(<RemindersPanel reminders={[]} onOpen={() => undefined} onCancel={() => undefined} />);
    expect(screen.queryByLabelText("Reminders")).toBeNull();
  });

  it("shows each reminder with its name, preview and due time", () => {
    render(<RemindersPanel reminders={reminders} onOpen={() => undefined} onCancel={() => undefined} />);
    expect(screen.getByText("Reminders (2)")).toBeTruthy();
    expect(screen.getByText("alice")).toBeTruthy();
    expect(screen.getByText("hello alice")).toBeTruthy();
    expect(screen.queryByText("second line")).toBeNull();
    expect(screen.getByText("# crew")).toBeTruthy();
    expect(screen.getByText("hello crew")).toBeTruthy();
    expect(screen.getByText("in 1h")).toBeTruthy();
    expect(screen.getByText("in 3h")).toBeTruthy();
    expect(screen.getByText("from alice")).toBeTruthy();
    expect(screen.getByText("from bob")).toBeTruthy();
  });

  it("opens a reminder and cancels one through the callbacks", () => {
    const onOpen = vi.fn();
    const onCancel = vi.fn();
    render(<RemindersPanel reminders={reminders} onOpen={onOpen} onCancel={onCancel} />);
    fireEvent.click(screen.getByText("alice"));
    expect(onOpen).toHaveBeenCalledWith(reminders[0]);
    fireEvent.click(screen.getByLabelText("Cancel reminder for crew"));
    expect(onCancel).toHaveBeenCalledWith(reminders[1]);
  });
});
