import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import Composer, { MAX_FILE_BYTES, validateFile, validateLocation, validatePoll } from "./Composer";
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

describe("validateLocation", () => {
  it("requires coordinates in range and a short place name", () => {
    expect(validateLocation("", "-83.88", "")).toMatch(/Latitude is required/);
    expect(validateLocation("north", "-83.88", "")).toMatch(/must be a number/);
    expect(validateLocation("91", "-83.88", "")).toMatch(/between -90 and 90/);
    expect(validateLocation("34.05", "", "")).toMatch(/Longitude is required/);
    expect(validateLocation("34.05", "181", "")).toMatch(/between -180 and 180/);
    expect(validateLocation("34.05", "-83.88", "x".repeat(201))).toMatch(/at most 200/);
    expect(validateLocation("34.05", "-83.88", "Rock Springs Park")).toBeNull();
    expect(validateLocation("34.05", "-83.88", "")).toBeNull();
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

  it("validates location pins before sending", async () => {
    const onSend = vi.fn();
    render(<Composer onSend={onSend} onSendFile={() => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "📍 Pin" }));
    fireEvent.click(screen.getByText("Send pin"));
    expect(screen.getByRole("alert").textContent).toMatch(/Latitude is required/);
    expect(onSend).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Latitude"), { target: { value: "34.05" } });
    fireEvent.change(screen.getByLabelText("Longitude"), { target: { value: "-83.88" } });
    fireEvent.change(screen.getByLabelText("Place name"), { target: { value: "Rock Springs Park" } });
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "meet here" } });
    fireEvent.click(screen.getByText("Send pin"));
    expect(onSend).toHaveBeenCalledWith(
      "meet here",
      expect.objectContaining({
        location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
      }),
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
});
