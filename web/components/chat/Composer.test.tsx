import { describe, expect, it, vi, afterEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import Composer, {
  MAX_FILE_BYTES,
  formatVoiceDuration,
  validateContact,
  validateFile,
  validateLocation,
  validatePoll,
} from "./Composer";
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

describe("validateContact", () => {
  it("requires a peer picked from the list", () => {
    expect(validateContact("")).toMatch(/Choose a peer/);
    expect(validateContact("cc".repeat(16))).toBeNull();
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

  it("shares a contact card from the picker", async () => {
    const onSend = vi.fn();
    render(
      <Composer
        contactPeers={[
          { key: "cc".repeat(16), name: "carol" },
          { key: "dd".repeat(16), name: "dave" },
        ]}
        onSend={onSend}
        onSendFile={() => undefined}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "👤 Contact" }));
    fireEvent.click(screen.getByText("Share contact"));
    expect(screen.getByRole("alert").textContent).toMatch(/Choose a peer/);
    expect(onSend).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Peer to share"), {
      target: { value: "cc".repeat(16) },
    });
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "meet carol" } });
    fireEvent.click(screen.getByText("Share contact"));
    expect(onSend).toHaveBeenCalledWith(
      "meet carol",
      expect.objectContaining({
        contact: { name: "carol", destination_hash: "cc".repeat(16) },
      }),
    );
  });

  it("clears the contact draft only when the send went through", async () => {
    const peers = [
      { key: "cc".repeat(16), name: "carol" },
      { key: "dd".repeat(16), name: "dave" },
    ];
    const share = async (onSend: (text: string, opts: any) => Promise<boolean>) => {
      const rendered = render(
        <Composer
          contactPeers={peers}
          onSend={onSend}
          onSendFile={() => undefined}
        />,
      );
      fireEvent.click(screen.getByRole("button", { name: "👤 Contact" }));
      fireEvent.change(screen.getByLabelText("Peer to share"), {
        target: { value: "cc".repeat(16) },
      });
      fireEvent.change(screen.getByLabelText("Message"), { target: { value: "meet carol" } });
      fireEvent.click(screen.getByText("Share contact"));
      return rendered;
    };

    // A confirmed send clears the text and leaves contact mode.
    const okSend = vi.fn().mockResolvedValue(true);
    const { unmount } = await share(okSend);
    await vi.waitFor(() =>
      expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe(""),
    );
    expect(screen.queryByLabelText("Peer to share")).toBeNull();
    unmount();

    // A failed send keeps the text and the picked peer for a retry.
    const failedSend = vi.fn().mockResolvedValue(false);
    await share(failedSend);
    await vi.waitFor(() => expect(failedSend).toHaveBeenCalled());
    expect((screen.getByLabelText("Message") as HTMLTextAreaElement).value).toBe(
      "meet carol",
    );
    expect(screen.getByLabelText("Peer to share")).toBeTruthy();
    expect(screen.getByText("Share contact")).toBeTruthy();
  });

  it("resets the contact draft when the conversation changes", () => {
    const peers = [{ key: "cc".repeat(16), name: "carol" }];
    const { rerender } = render(
      <Composer
        contactPeers={peers}
        conversationKey="alice"
        onSend={() => undefined}
        onSendFile={() => undefined}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "👤 Contact" }));
    fireEvent.change(screen.getByLabelText("Peer to share"), {
      target: { value: "cc".repeat(16) },
    });
    expect(screen.getByLabelText("Peer to share")).toBeTruthy();

    // Switching threads drops the picked card instead of carrying it
    // into the new recipient's composer.
    rerender(
      <Composer
        contactPeers={peers}
        conversationKey="bob"
        onSend={() => undefined}
        onSendFile={() => undefined}
      />,
    );
    expect(screen.queryByLabelText("Peer to share")).toBeNull();
  });

  it("hides the Contact toggle without known peers", () => {
    render(<Composer onSend={() => undefined} onSendFile={() => undefined} />);
    expect(screen.queryByRole("button", { name: "👤 Contact" })).toBeNull();
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

describe("formatVoiceDuration", () => {
  it("formats elapsed seconds as m:ss", () => {
    expect(formatVoiceDuration(0)).toBe("0:00");
    expect(formatVoiceDuration(7)).toBe("0:07");
    expect(formatVoiceDuration(65)).toBe("1:05");
    expect(formatVoiceDuration(600)).toBe("10:00");
  });
});

describe("voice recording", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function installVoiceMocks(opts?: { deferPermission?: boolean }) {
    const stopTrack = vi.fn();
    const stream = {
      getTracks: () => [{ stop: stopTrack }],
    } as unknown as MediaStream;
    let resolvePermission: ((s: MediaStream) => void) | undefined;
    const getUserMedia = vi.fn().mockImplementation(() => {
      if (opts?.deferPermission) {
        return new Promise<MediaStream>((resolve) => {
          resolvePermission = resolve;
        });
      }
      return Promise.resolve(stream);
    });
    Object.defineProperty(navigator, "mediaDevices", {
      value: { getUserMedia },
      configurable: true,
    });
    const recorders: {
      ondataavailable: ((e: { data: Blob }) => void) | null;
      onstop: (() => void) | null;
      onerror: ((e: unknown) => void) | null;
      stop: () => void;
    }[] = [];
    class FakeRecorder {
      static isTypeSupported = () => true;
      ondataavailable: ((e: { data: Blob }) => void) | null = null;
      onstop: (() => void) | null = null;
      onerror: ((e: unknown) => void) | null = null;
      mimeType = "audio/webm;codecs=opus";
      state = "inactive";
      constructor(
        public stream: MediaStream,
        public opts?: { mimeType?: string },
      ) {
        recorders.push(this);
      }
      start() {
        this.state = "recording";
        this.ondataavailable?.({ data: new Blob(["audio"], { type: "audio/webm" }) });
      }
      stop() {
        this.state = "inactive";
        this.onstop?.();
      }
    }
    vi.stubGlobal("MediaRecorder", FakeRecorder);
    return {
      getUserMedia,
      stopTrack,
      recorders,
      resolvePermission: () => resolvePermission?.(stream),
    };
  }

  it("hides the mic button when the browser cannot record", () => {
    render(<Composer onSend={() => undefined} onSendFile={() => undefined} />);
    expect(screen.queryByLabelText("Record a voice note")).toBeNull();
  });

  it("records and sends a voice note with the voice marker", async () => {
    const { getUserMedia } = installVoiceMocks();
    const onSendFile = vi.fn();
    render(<Composer onSend={() => undefined} onSendFile={onSendFile} />);

    fireEvent.click(screen.getByLabelText("Record a voice note"));
    await vi.waitFor(() =>
      expect(screen.getByLabelText("Stop recording")).toBeTruthy(),
    );
    expect(getUserMedia).toHaveBeenCalledWith({ audio: true });

    fireEvent.click(screen.getByLabelText("Stop recording"));
    await vi.waitFor(() => expect(screen.getByText(/Voice note:/)).toBeTruthy());

    fireEvent.click(screen.getByText("Send"));
    await vi.waitFor(() =>
      expect(onSendFile).toHaveBeenCalledWith(
        expect.objectContaining({ name: expect.stringMatching(/^voice-note-.*\.webm$/) }),
        expect.objectContaining({ voice: true }),
      ),
    );
  });

  it("does not start recording when the chat locks while permission is pending", async () => {
    const mocks = installVoiceMocks({ deferPermission: true });
    const { rerender } = render(
      <Composer disabled={false} onSend={() => undefined} onSendFile={() => undefined} />,
    );

    fireEvent.click(screen.getByLabelText("Record a voice note"));
    expect(mocks.getUserMedia).toHaveBeenCalledWith({ audio: true });

    // The announcements-only group locks while the prompt is open.
    rerender(
      <Composer disabled onSend={() => undefined} onSendFile={() => undefined} />,
    );
    mocks.resolvePermission();
    await vi.waitFor(() => expect(mocks.stopTrack).toHaveBeenCalled());
    expect(screen.queryByLabelText("Stop recording")).toBeNull();
  });

  it("drops a recording that finishes after the conversation changed", async () => {
    const mocks = installVoiceMocks();
    const onSendFile = vi.fn();
    const { rerender } = render(
      <Composer
        conversationKey="chat-a"
        onSend={() => undefined}
        onSendFile={onSendFile}
      />,
    );

    fireEvent.click(screen.getByLabelText("Record a voice note"));
    await vi.waitFor(() =>
      expect(screen.getByLabelText("Stop recording")).toBeTruthy(),
    );

    // Switch chats, then let the old recorder's stop land late: the
    // cleanup effect normally discards first, and the session-key check
    // in onstop covers the window before the effect runs.
    rerender(
      <Composer
        conversationKey="chat-b"
        onSend={() => undefined}
        onSendFile={onSendFile}
      />,
    );
    mocks.recorders[0].stop();
    expect(screen.queryByText(/Voice note:/)).toBeNull();
    expect(onSendFile).not.toHaveBeenCalled();
  });

  it("sends a picked file without the voice marker", async () => {
    installVoiceMocks();
    const onSendFile = vi.fn();
    render(<Composer onSend={() => undefined} onSendFile={onSendFile} />);
    const input = screen.getByLabelText("Attach a file") as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [new File(["hello"], "notes.txt")] },
    });
    expect(screen.getByText(/Attached:/)).toBeTruthy();
    fireEvent.click(screen.getByText("Send"));
    await vi.waitFor(() =>
      expect(onSendFile).toHaveBeenCalledWith(
        expect.objectContaining({ name: "notes.txt" }),
        expect.objectContaining({ voice: false }),
      ),
    );
  });
});
