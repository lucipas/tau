import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TauMessage } from "@/lib/types";

import { Composer } from "./composer";

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "out",
    peer: "ab".repeat(16),
    peer_name: "alice",
    text: "Sent file: notes.txt (5 bytes)",
    sent_at: 1000,
    received_at: 1001,
    status: "delivered",
    ...overrides,
  };
}

function renderComposer({
  onSendFile = vi.fn().mockResolvedValue(makeMessage()),
  onSend = vi.fn().mockResolvedValue(makeMessage()),
} = {}) {
  render(
    <Composer
      peers={[]}
      target={"ab".repeat(16)}
      mode="custom"
      onTargetChange={() => {}}
      onModeChange={() => {}}
      onSend={onSend}
      onSendFile={onSendFile}
    />,
  );
  return { onSend, onSendFile };
}

function attachFile(file: File) {
  const input = screen.getByLabelText("Attach a file");
  fireEvent.change(input, { target: { files: [file] } });
}

describe("Composer file attach", () => {
  it("shows the picked file and sends it to the selected peer", async () => {
    const { onSendFile } = renderComposer();
    const file = new File(["hello"], "notes.txt", { type: "text/plain" });
    attachFile(file);

    expect(screen.getByText(/notes\.txt/)).toBeTruthy();
    expect(screen.getByText(/5 B/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /send file/i }));
    await waitFor(() =>
      expect(onSendFile).toHaveBeenCalledWith("ab".repeat(16), file),
    );
    await waitFor(() =>
      expect(screen.queryByText(/notes\.txt/)).toBeNull(),
    );
  });

  it("refuses a file larger than 10 MiB without sending", async () => {
    const { onSendFile } = renderComposer();
    const file = new File(["x"], "big.bin", {
      type: "application/octet-stream",
    });
    Object.defineProperty(file, "size", { value: 10 * 1024 * 1024 + 1 });
    attachFile(file);

    expect(screen.getByRole("alert").textContent).toBe(
      "File is larger than 10 MiB.",
    );
    expect(onSendFile).not.toHaveBeenCalled();
  });

  it("still sends text when no file is attached", async () => {
    const { onSend } = renderComposer();
    const textarea = screen.getByPlaceholderText(/Write something/);
    fireEvent.change(textarea, { target: { value: "hello" } });

    fireEvent.click(screen.getByRole("button", { name: /^send$/i }));
    await waitFor(() =>
      expect(onSend).toHaveBeenCalledWith(
        "ab".repeat(16),
        "hello",
        undefined,
        undefined,
      ),
    );
  });
});
