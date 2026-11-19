import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import Composer, { MAX_FILE_BYTES, validateFile, validatePoll } from "./Composer";
describe("validateFile", () => {
  it("accepts files at or under 10 MiB", () => {
    const ok = new File([new ArrayBuffer(MAX_FILE_BYTES)], "ok.bin");
    expect(validateFile(ok)).toBeNull();
  });

  it("rejects files over 10 MiB", () => {
    const big = new File([new ArrayBuffer(MAX_FILE_BYTES + 1)], "big.bin");
    expect(validateFile(big)).toMatch(/10\.0 MiB/);
  });

  it("rejects empty files", () => {
    expect(validateFile(new File([], "empty.bin"))).toMatch(/empty/);
  });
});

describe("validatePoll", () => {
  it("requires a question and two options", () => {
    expect(validatePoll("", ["a", "b"])).toMatch(/question/);
    expect(validatePoll("q?", ["only one", ""])).toMatch(/two options/);
    expect(validatePoll("q?", ["a", "b"])).toBeNull();
  });
});

describe("Composer", () => {
  it("keeps Send disabled until there is text", () => {
    render(<Composer onSend={() => undefined} onSendFile={() => undefined} />);
    expect((screen.getByText("Send") as HTMLButtonElement).disabled).toBe(true);
  });

  it("sends typed text and clears the box", async () => {
    const onSend = vi.fn();
    render(<Composer onSend={onSend} onSendFile={() => undefined} />);
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "hello mesh" } });
    fireEvent.click(screen.getByText("Send"));
    expect(onSend).toHaveBeenCalledWith("hello mesh", expect.objectContaining({}));
    await vi.waitFor(() =>
      expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe(""),
    );
  });

  it("shows an error for an oversize file instead of attaching it", () => {
    const onSendFile = vi.fn();
    render(<Composer onSend={() => undefined} onSendFile={onSendFile} />);
    const input = screen.getByLabelText("Attach a file") as HTMLInputElement;
    const big = new File([new ArrayBuffer(MAX_FILE_BYTES + 1)], "big.bin");
    fireEvent.change(input, { target: { files: [big] } });
    expect(screen.getByRole("alert").textContent).toMatch(/10\.0 MiB/);
    expect(screen.queryByText(/Attached:/)).toBeNull();
  });

  it("validates polls before sending", async () => {
    const onSend = vi.fn();
    render(<Composer allowPoll onSend={onSend} onSendFile={() => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: /Poll/ }));
    fireEvent.change(screen.getByLabelText("Poll question"), { target: { value: "Best mesh?" } });
    fireEvent.click(screen.getByText("Post poll"));
    expect(screen.getByRole("alert").textContent).toMatch(/two options/);
    expect(onSend).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Poll option 1"), { target: { value: "Reticulum" } });
    fireEvent.change(screen.getByLabelText("Poll option 2"), { target: { value: "LoRa" } });
    fireEvent.click(screen.getByText("Post poll"));
    expect(onSend).toHaveBeenCalledWith(
      "Best mesh?",
      expect.objectContaining({ pollOptions: ["Reticulum", "LoRa"] }),
    );
  });

  it("passes the chosen disappearing expiry to onSend", async () => {
    const onSend = vi.fn();
    render(<Composer onSend={onSend} onSendFile={() => undefined} />);
    fireEvent.change(screen.getByLabelText("Disappearing message expiry"), { target: { value: "3600" } });
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "vanish soon" } });
    fireEvent.click(screen.getByText("Send"));
    expect(onSend).toHaveBeenCalledWith("vanish soon", expect.objectContaining({ expiresIn: 3600 }));
  });

  it("hides Save draft without an onSaveDraft handler", () => {
    render(<Composer onSend={() => undefined} onSendFile={() => undefined} />);
    expect(screen.queryByText("Save draft")).toBeNull();
  });

  it("saves a draft and clears the box", async () => {
    const onSaveDraft = vi.fn().mockResolvedValue(undefined);
    render(<Composer onSend={() => undefined} onSendFile={() => undefined} onSaveDraft={onSaveDraft} />);
    const save = screen.getByText("Save draft") as HTMLButtonElement;
    expect(save.disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "park me" } });
    expect(save.disabled).toBe(false);
    fireEvent.click(save);
    expect(onSaveDraft).toHaveBeenCalledWith("park me");
    await vi.waitFor(() =>
      expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe(""),
    );
  });

  it("keeps the text when saving a draft fails", async () => {
    const onSaveDraft = vi.fn().mockRejectedValue(new Error("boom"));
    render(<Composer onSend={() => undefined} onSendFile={() => undefined} onSaveDraft={onSaveDraft} />);
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "keep me" } });
    fireEvent.click(screen.getByText("Save draft"));
    await vi.waitFor(() => expect(screen.getByRole("alert").textContent).toMatch(/Save draft failed/));
    expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe("keep me");
  });

  it("pre-fills the box from a draft seed, but never clobbers typed text", () => {
    const { rerender } = render(
      <Composer onSend={() => undefined} onSendFile={() => undefined} />,
    );
    rerender(
      <Composer
        onSend={() => undefined}
        onSendFile={() => undefined}
        draftSeed={{ key: "dm:aa", text: "saved hello" }}
      />,
    );
    expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe("saved hello");

    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "my own words" } });
    rerender(
      <Composer
        onSend={() => undefined}
        onSendFile={() => undefined}
        draftSeed={{ key: "dm:bb", text: "another draft" }}
      />,
    );
    expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe("my own words");
  });
});
