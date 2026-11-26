// TanComposer.test.tsx — image attach publishes with the post, and a
// scheduled post can carry one too; replies and polls cannot. The
// composer prop is a fake, so no fetch stub is needed.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import TanComposer from "./TanComposer";
import type { UseTanComposerResult } from "@/lib/use-tan";
import type { TanPost } from "@/lib/types";

function makeComposer(): UseTanComposerResult & {
  publish: ReturnType<typeof vi.fn>;
  schedule: ReturnType<typeof vi.fn>;
  schedulePoll: ReturnType<typeof vi.fn>;
  poll: ReturnType<typeof vi.fn>;
} {
  return {
    publish: vi.fn().mockResolvedValue({} as TanPost),
    schedule: vi.fn().mockResolvedValue({}),
    schedulePoll: vi.fn().mockResolvedValue({}),
    poll: vi.fn().mockResolvedValue({} as TanPost),
    busy: false,
    error: null,
  };
}

function pickImage(name = "pic.png", type = "image/png", bytes = "hello") {
  const input = screen.getByLabelText("Attach an image") as HTMLInputElement;
  const file = new File([bytes], name, { type });
  fireEvent.change(input, { target: { files: [file] } });
  return file;
}

function typePost(text: string) {
  fireEvent.change(screen.getByLabelText("Compose a post"), {
    target: { value: text },
  });
}

function submit() {
  fireEvent.click(screen.getByRole("button", { name: "Post" }));
}

describe("TanComposer image attach", () => {
  it("publishes a picked image with the post as a data URL", async () => {
    const composer = makeComposer();
    const onPosted = vi.fn();
    render(<TanComposer composer={composer} onPosted={onPosted} />);
    pickImage();
    typePost("with a picture");
    submit();
    await waitFor(() =>
      expect(composer.publish).toHaveBeenCalledWith(
        "with a picture",
        expect.objectContaining({
          image: { dataUrl: "data:image/png;base64,aGVsbG8=" },
        }),
      ),
    );
    expect(onPosted).toHaveBeenCalled();
  });

  it("posts text-only once the attached file is removed", async () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} />);
    pickImage();
    expect(screen.getByText(/Attached:/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    typePost("text only");
    submit();
    await waitFor(() =>
      expect(composer.publish).toHaveBeenCalledWith(
        "text only",
        expect.objectContaining({ image: undefined }),
      ),
    );
  });

  it("disables the attach picker while replying and drops a picked file", () => {
    const composer = makeComposer();
    const { rerender } = render(<TanComposer composer={composer} />);
    pickImage();
    expect(screen.getByText(/Attached:/)).toBeTruthy();
    rerender(<TanComposer composer={composer} replyTo={"ab".repeat(32)} />);
    expect(screen.queryByText(/Attached:/)).toBeNull();
    const attach = screen.getByRole("button", { name: "🖼 Attach" });
    expect(attach.hasAttribute("disabled")).toBe(true);
    expect(attach.getAttribute("title")).toMatch(/Replies can't carry an image/);
  });

  it("disables the attach picker in poll mode and drops a picked file", () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} />);
    pickImage();
    fireEvent.click(screen.getByRole("button", { name: "📊 Poll" }));
    expect(screen.queryByText(/Attached:/)).toBeNull();
    const attach = screen.getByRole("button", { name: "🖼 Attach" });
    expect(attach.hasAttribute("disabled")).toBe(true);
    expect(attach.getAttribute("title")).toMatch(/Polls can't carry an image/);
  });

  it("schedules a post with an image attached", async () => {
    const composer = makeComposer();
    const onPosted = vi.fn();
    render(<TanComposer composer={composer} onPosted={onPosted} />);
    pickImage();
    typePost("later with pic");
    fireEvent.change(screen.getByLabelText("Send later"), {
      target: { value: "300" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Schedule" }));
    await waitFor(() =>
      expect(composer.schedule).toHaveBeenCalledWith(
        "later with pic",
        300,
        expect.objectContaining({
          image: { dataUrl: "data:image/png;base64,aGVsbG8=" },
        }),
      ),
    );
    expect(composer.publish).not.toHaveBeenCalled();
    expect(onPosted).toHaveBeenCalled();
  });

  it("refuses an image over the 2 MB limit", async () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} />);
    const input = screen.getByLabelText("Attach an image") as HTMLInputElement;
    const big = new File([new Uint8Array(2 * 1024 * 1024 + 1)], "big.png", {
      type: "image/png",
    });
    fireEvent.change(input, { target: { files: [big] } });
    typePost("too big");
    submit();
    expect((await screen.findByRole("alert")).textContent).toMatch(/2 MB limit/);
    expect(composer.publish).not.toHaveBeenCalled();
  });

  it("schedules a poll instead of posting it when Send later is set", async () => {
    const composer = makeComposer();
    const onPosted = vi.fn();
    render(<TanComposer composer={composer} onPosted={onPosted} />);
    fireEvent.click(screen.getByRole("button", { name: "📊 Poll" }));
    fireEvent.change(screen.getByLabelText("Poll question"), {
      target: { value: "pizza or tacos?" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 1"), {
      target: { value: "Pizza" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 2"), {
      target: { value: "Tacos" },
    });
    fireEvent.change(screen.getByLabelText("Send later"), {
      target: { value: "300" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Schedule" }));
    await waitFor(() =>
      expect(composer.schedulePoll).toHaveBeenCalledWith(
        "pizza or tacos?",
        ["Pizza", "Tacos"],
        300,
        { expiresIn: undefined },
      ),
    );
    expect(composer.poll).not.toHaveBeenCalled();
    expect(onPosted).toHaveBeenCalled();
  });

  it("posts a poll immediately when Send later is not set", async () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} />);
    fireEvent.click(screen.getByRole("button", { name: "📊 Poll" }));
    fireEvent.change(screen.getByLabelText("Poll question"), {
      target: { value: "pizza or tacos?" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 1"), {
      target: { value: "Pizza" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 2"), {
      target: { value: "Tacos" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Post poll" }));
    await waitFor(() =>
      expect(composer.poll).toHaveBeenCalledWith(
        "pizza or tacos?",
        ["Pizza", "Tacos"],
        undefined,
      ),
    );
    expect(composer.schedulePoll).not.toHaveBeenCalled();
  });
});

describe("TanComposer drafts", () => {
  it("saves the text as a draft and clears the composer", async () => {
    const composer = makeComposer();
    const onSaveDraft = vi.fn().mockResolvedValue(undefined);
    render(<TanComposer composer={composer} onSaveDraft={onSaveDraft} />);
    typePost("half-written thought");
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() =>
      expect(onSaveDraft).toHaveBeenCalledWith("half-written thought", {
        replyTo: undefined,
        expiresIn: undefined,
      }),
    );
    expect(
      (screen.getByLabelText("Compose a post") as HTMLTextAreaElement).value,
    ).toBe("");
    expect(composer.publish).not.toHaveBeenCalled();
  });

  it("carries reply and expiry into the saved draft", async () => {
    const composer = makeComposer();
    const onSaveDraft = vi.fn().mockResolvedValue(undefined);
    render(
      <TanComposer
        composer={composer}
        replyTo={"ab".repeat(32)}
        onSaveDraft={onSaveDraft}
      />,
    );
    typePost("agreed");
    fireEvent.change(screen.getByLabelText("Post expiry"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() =>
      expect(onSaveDraft).toHaveBeenCalledWith("agreed", {
        replyTo: "ab".repeat(32),
        expiresIn: 3600,
      }),
    );
  });

  it("prefills text and expiry from a loaded draft", () => {
    const composer = makeComposer();
    render(
      <TanComposer
        composer={composer}
        initialText="back to this later"
        initialExpiresIn={3600}
      />,
    );
    expect(
      (screen.getByLabelText("Compose a post") as HTMLTextAreaElement).value,
    ).toBe("back to this later");
    expect(
      (screen.getByLabelText("Post expiry") as HTMLSelectElement).value,
    ).toBe("3600");
  });

  it("disables Save draft in poll mode and hides it without a handler", () => {
    const composer = makeComposer();
    const onSaveDraft = vi.fn().mockResolvedValue(undefined);
    render(<TanComposer composer={composer} onSaveDraft={onSaveDraft} />);
    fireEvent.click(screen.getByRole("button", { name: "📊 Poll" }));
    const save = screen.getByRole("button", { name: "Save draft" });
    expect(save.hasAttribute("disabled")).toBe(true);
    expect(save.getAttribute("title")).toMatch(/Polls can't be saved as drafts/);
  });

  it("shows no Save draft button when the parent cannot save drafts", () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} />);
    expect(screen.queryByRole("button", { name: "Save draft" })).toBeNull();
  });
});

describe("TanComposer contact attach", () => {
  const PEERS = [{ key: "cc".repeat(16), name: "carol" }];
  const CARD = { name: "carol", destination_hash: "cc".repeat(16) };

  function pickContact() {
    fireEvent.click(screen.getByRole("button", { name: "👤 Contact" }));
    fireEvent.change(screen.getByLabelText("Peer to share"), {
      target: { value: "cc".repeat(16) },
    });
  }

  it("publishes a picked contact card with the post", async () => {
    const composer = makeComposer();
    const onPosted = vi.fn();
    render(<TanComposer composer={composer} onPosted={onPosted} contactPeers={PEERS} />);
    pickContact();
    expect(screen.getByText(/Sharing:/)).toBeTruthy();
    typePost("talk to carol about this");
    submit();
    await waitFor(() =>
      expect(composer.publish).toHaveBeenCalledWith(
        "talk to carol about this",
        expect.objectContaining({ contact: CARD }),
      ),
    );
    expect(onPosted).toHaveBeenCalled();
  });

  it("posts text-only once the picked peer is removed", async () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} contactPeers={PEERS} />);
    pickContact();
    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    typePost("text only");
    submit();
    await waitFor(() =>
      expect(composer.publish).toHaveBeenCalledWith(
        "text only",
        expect.objectContaining({ contact: undefined }),
      ),
    );
  });

  it("hides the Contact toggle when there are no known peers", () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} contactPeers={[]} />);
    expect(screen.queryByRole("button", { name: "👤 Contact" })).toBeNull();
  });

  it("disables the contact picker while replying and drops a picked peer", () => {
    const composer = makeComposer();
    const { rerender } = render(
      <TanComposer composer={composer} contactPeers={PEERS} />,
    );
    pickContact();
    expect(screen.getByText(/Sharing:/)).toBeTruthy();
    rerender(
      <TanComposer
        composer={composer}
        contactPeers={PEERS}
        replyTo={"ab".repeat(32)}
      />,
    );
    expect(screen.queryByText(/Sharing:/)).toBeNull();
    const contact = screen.getByRole("button", { name: "👤 Contact" });
    expect(contact.hasAttribute("disabled")).toBe(true);
    expect(contact.getAttribute("title")).toMatch(/Replies can't carry a contact card/);
  });

  it("disables the contact picker in poll mode and drops a picked peer", () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} contactPeers={PEERS} />);
    pickContact();
    fireEvent.click(screen.getByRole("button", { name: "📊 Poll" }));
    expect(screen.queryByText(/Sharing:/)).toBeNull();
    const contact = screen.getByRole("button", { name: "👤 Contact" });
    expect(contact.hasAttribute("disabled")).toBe(true);
    expect(contact.getAttribute("title")).toMatch(/Polls can't carry a contact card/);
  });

  it("schedules a post with a contact card attached", async () => {
    const composer = makeComposer();
    const onPosted = vi.fn();
    render(<TanComposer composer={composer} onPosted={onPosted} contactPeers={PEERS} />);
    pickContact();
    typePost("later with carol");
    fireEvent.change(screen.getByLabelText("Send later"), {
      target: { value: "300" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Schedule" }));
    await waitFor(() =>
      expect(composer.schedule).toHaveBeenCalledWith(
        "later with carol",
        300,
        expect.objectContaining({ contact: CARD }),
      ),
    );
    expect(composer.publish).not.toHaveBeenCalled();
    expect(onPosted).toHaveBeenCalled();
  });
});
