// PagesSection.test.tsx — page list, viewer, fetch-a-page.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import PagesSection, { renderPageText } from "./PagesSection";
import { mockFetch } from "./test-utils";

let calls: string[] = [];

const page = {
  author: "e".repeat(32),
  title: "Eve's page",
  markdown: "# Hello\n\nWelcome to my mesh page.",
  files: [{ name: "photo.jpg", sha256: "f".repeat(64), size: 1234 }],
  version: 2,
  updated_at: 1000,
};

beforeEach(() => {
  const router = mockFetch((url, method) => {
    if (url.includes("/api/pages/fetch") && method === "POST") return { page };
    if (url.includes("/api/pages")) {
      return { pages: [page], mine: null, count: 1 };
    }
    return { ok: true };
  });
  calls = router.calls;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("renderPageText", () => {
  it("splits markdown into heading and paragraph blocks as plain text", () => {
    const blocks = renderPageText("# Title\n\nSome <b>bold</b> text.");
    expect(blocks).toHaveLength(2);
    expect(blocks[0]).toEqual({ key: "block-0", text: "Title", heading: true });
    expect(blocks[1].text).toBe("Some <b>bold</b> text.");
  });
});

describe("PagesSection", () => {
  it("lists cached peer pages", async () => {
    render(<PagesSection />);
    expect(await screen.findByText("Eve's page")).toBeTruthy();
    expect(screen.getByText(/Pages from the mesh/)).toBeTruthy();
  });

  it("opens the viewer with page text and file links", async () => {
    render(<PagesSection />);
    fireEvent.click(await screen.findByText("Eve's page"));
    expect(await screen.findByText("Welcome to my mesh page.")).toBeTruthy();
    const link = screen.getByRole("link", { name: "Download" });
    expect(link.getAttribute("href")).toContain(
      `/api/pages/${"e".repeat(32)}/files/${"f".repeat(64)}`,
    );
    fireEvent.click(screen.getByRole("button", { name: /All pages/ }));
    expect(await screen.findByText("Eve's page")).toBeTruthy();
  });

  it("fetches a peer page by destination hash", async () => {
    render(<PagesSection />);
    await screen.findByText("Eve's page");
    fireEvent.change(screen.getByLabelText("Peer destination hash to fetch"), {
      target: { value: "e".repeat(32) },
    });
    fireEvent.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(calls).toContain("POST /api/pages/fetch"));
    expect(await screen.findByText(/Fetched Eve's page/)).toBeTruthy();
  });
});
