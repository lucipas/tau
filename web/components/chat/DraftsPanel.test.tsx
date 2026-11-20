import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import DraftsPanel from "./DraftsPanel";
import type { MessageDraft } from "@/lib/types";

const drafts: MessageDraft[] = [
  { kind: "peer", target: "ab".repeat(16), text: "hello alice", updated_at: 100, name: "alice" },
  { kind: "group", target: "cd".repeat(16), text: "hello crew\nsecond line", updated_at: 200, name: "crew" },
  { kind: "peer", target: "ef".repeat(16), text: "stranger", updated_at: 300, name: null },
];

describe("DraftsPanel", () => {
  it("renders nothing when there are no drafts", () => {
    render(<DraftsPanel drafts={[]} onOpen={() => undefined} onDelete={() => undefined} />);
    expect(screen.queryByLabelText("Drafts")).toBeNull();
  });

  it("shows each draft with its name and first-line preview", () => {
    render(<DraftsPanel drafts={drafts} onOpen={() => undefined} onDelete={() => undefined} />);
    expect(screen.getByText("Drafts (3)")).toBeTruthy();
    expect(screen.getByText("alice")).toBeTruthy();
    expect(screen.getByText("hello alice")).toBeTruthy();
    expect(screen.getByText("# crew")).toBeTruthy();
    expect(screen.getByText("hello crew")).toBeTruthy();
    expect(screen.queryByText("second line")).toBeNull();
  });

  it("falls back to a truncated hash for an unnamed draft", () => {
    render(<DraftsPanel drafts={drafts} onOpen={() => undefined} onDelete={() => undefined} />);
    expect(screen.getByText("efefefef…")).toBeTruthy();
  });

  it("opens a draft and deletes one through the callbacks", () => {
    const onOpen = vi.fn();
    const onDelete = vi.fn();
    render(<DraftsPanel drafts={drafts} onOpen={onOpen} onDelete={onDelete} />);
    fireEvent.click(screen.getByText("alice"));
    expect(onOpen).toHaveBeenCalledWith(drafts[0]);
    fireEvent.click(screen.getByLabelText("Delete draft for crew"));
    expect(onDelete).toHaveBeenCalledWith(drafts[1]);
  });

  it("shows a channel draft with a # prefix", () => {
    const channelDraft: MessageDraft = {
      kind: "channel",
      target: "general",
      text: "hello channel",
      updated_at: 400,
      name: "general",
    };
    render(<DraftsPanel drafts={[channelDraft]} onOpen={() => undefined} onDelete={() => undefined} />);
    expect(screen.getByText("Drafts (1)")).toBeTruthy();
    expect(screen.getByText("# general")).toBeTruthy();
    expect(screen.getByText("hello channel")).toBeTruthy();
  });
});
