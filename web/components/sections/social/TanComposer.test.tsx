// TanComposer.test.tsx — image attach publishes with the post; replies,
// polls and scheduled posts cannot carry one. The composer prop is a fake,
// so no fetch stub is needed.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import TanComposer from "./TanComposer";
import type { UseTanComposerResult } from "@/lib/use-tan";
import type { TanPost } from "@/lib/types";

function makeComposer(): UseTanComposerResult & {
  publish: ReturnType<typeof vi.fn>;
  schedule: ReturnType<typeof vi.fn>;
  poll: ReturnType<typeof vi.fn>;
} {
  return {
    publish: vi.fn().mockResolvedValue({} as TanPost),
    schedule: vi.fn().mockResolvedValue({}),
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

  it("refuses to schedule a post with an image attached", async () => {
    const composer = makeComposer();
    render(<TanComposer composer={composer} />);
    pickImage();
    typePost("later with pic");
    fireEvent.change(screen.getByLabelText("Send later"), {
      target: { value: "300" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Schedule" }));
    expect((await screen.findByRole("alert")).textContent).toMatch(/can't be scheduled/);
    expect(composer.publish).not.toHaveBeenCalled();
    expect(composer.schedule).not.toHaveBeenCalled();
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
});
