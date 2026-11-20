import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import OutboxPanel from "./OutboxPanel";
import type { Outbox, OutboxEntry } from "@/lib/types";

function entry(overrides: Partial<OutboxEntry> = {}): OutboxEntry {
  return {
    id: "aa".repeat(16),
    to: "bb".repeat(16),
    to_name: "alice",
    text: "see you soon",
    kind: "chat",
    created_at: 1000,
    expires_at: 2000,
    attempts: 0,
    last_attempt_at: null,
    last_error: null,
    next_attempt_at: null,
    held_by: [],
    message_expires_at: null,
    send_after: Date.now() / 1000 + 3600,
    sent_at: null,
    edited_at: null,
    ...overrides,
  };
}

const scheduled: Outbox = {
  outbox: [entry()],
  count: 1,
  held_for_others: 0,
};

function renderPanel(outbox: Outbox | null = scheduled, overrides = {}) {
  const props = {
    outbox,
    held: [],
    onCancel: () => undefined,
    onEdit: () => undefined,
    onRetry: () => undefined,
    onDropHeld: () => undefined,
    ...overrides,
  };
  return render(<OutboxPanel {...props} />);
}

describe("OutboxPanel", () => {
  it("shows an Edit button only for a scheduled plain message", () => {
    renderPanel({
      outbox: [
        entry(),
        entry({
          id: "cc".repeat(16),
          text: "already due",
          send_after: null,
        }),
        entry({
          id: "dd".repeat(16),
          text: "lunch?",
          poll_options: ["yes", "no"],
        }),
      ],
      count: 3,
      held_for_others: 0,
    });
    expect(screen.getAllByLabelText("Edit scheduled message")).toHaveLength(1);
  });

  it("opens an edit form prefilled with the text and saves a new text", () => {
    const onEdit = vi.fn();
    renderPanel(scheduled, { onEdit });
    fireEvent.click(screen.getByLabelText("Edit scheduled message"));
    const input = screen.getByLabelText("Edit scheduled message text");
    expect((input as HTMLInputElement).value).toBe("see you soon");
    fireEvent.change(input, { target: { value: "see you at noon" } });
    fireEvent.click(screen.getByText("Save"));
    expect(onEdit).toHaveBeenCalledWith("aa".repeat(16), {
      text: "see you at noon",
    });
  });

  it("saves a new send time without touching the text", () => {
    const onEdit = vi.fn();
    renderPanel(scheduled, { onEdit });
    fireEvent.click(screen.getByLabelText("Edit scheduled message"));
    fireEvent.change(screen.getByLabelText("New send time"), {
      target: { value: "300" },
    });
    fireEvent.click(screen.getByText("Save"));
    expect(onEdit).toHaveBeenCalledWith("aa".repeat(16), { send_in: 300 });
  });

  it("makes no request for an unchanged save", () => {
    const onEdit = vi.fn();
    renderPanel(scheduled, { onEdit });
    fireEvent.click(screen.getByLabelText("Edit scheduled message"));
    expect((screen.getByText("Save") as HTMLButtonElement).disabled).toBe(
      true,
    );
    fireEvent.click(screen.getByText("Save"));
    expect(onEdit).not.toHaveBeenCalled();
  });

  it("closes the form without saving on Cancel", () => {
    const onEdit = vi.fn();
    renderPanel(scheduled, { onEdit });
    fireEvent.click(screen.getByLabelText("Edit scheduled message"));
    fireEvent.change(screen.getByLabelText("Edit scheduled message text"), {
      target: { value: "changed, but discarded" },
    });
    fireEvent.click(screen.getByText("Cancel"));
    expect(onEdit).not.toHaveBeenCalled();
    expect(
      screen.queryByLabelText("Edit scheduled message text"),
    ).toBeNull();
  });
});
