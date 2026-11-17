import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { Markdown } from "./markdown";

describe("Markdown", () => {
  it("renders headings, bold, italic, inline code and links", () => {
    render(
      <Markdown
        text={
          "# Big title\n\n## Smaller\n\nHello **bold** and *italic* and `code` and [a link](https://example.com)."
        }
      />,
    );
    expect(
      screen.getByRole("heading", { level: 1, name: "Big title" }),
    ).toBeDefined();
    expect(
      screen.getByRole("heading", { level: 2, name: "Smaller" }),
    ).toBeDefined();
    expect(screen.getByText("bold").tagName).toBe("STRONG");
    expect(screen.getByText("italic").tagName).toBe("EM");
    expect(screen.getByText("code").tagName).toBe("CODE");
    const link = screen.getByRole("link", { name: "a link" });
    expect(link.getAttribute("href")).toBe("https://example.com");
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel")).toBe("noreferrer");
  });

  it("renders bullet lists and fenced code blocks", () => {
    const { container } = render(
      <Markdown
        text={"- one\n- two\n\n```\ncode block\n```"}
      />,
    );
    expect(screen.getByText("one")).toBeDefined();
    expect(screen.getByText("two")).toBeDefined();
    expect(container.querySelector("pre")).not.toBeNull();
    expect(container.querySelector("pre")?.textContent).toContain(
      "code block",
    );
  });

  it("never renders raw HTML from the source as elements", () => {
    const { container } = render(
      <Markdown
        text={
          '# Title\n\n<script>alert("pwned")</script>\n\n<img src="x" onerror="alert(1)">'
        }
      />,
    );
    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    // The hostile markup survives only as inert text.
    expect(container.textContent).toContain('alert("pwned")');
    expect(
      screen.getByRole("heading", { level: 1, name: "Title" }),
    ).toBeDefined();
  });

  it("does not make javascript: links clickable", () => {
    const { container } = render(
      <Markdown text={"[click me](javascript:alert(1))"} />,
    );
    expect(container.querySelector("a")).toBeNull();
    expect(screen.getByText(/click me/)).toBeDefined();
  });

  it("renders blockquotes and horizontal rules", () => {
    const { container } = render(
      <Markdown text={"> quoted line\n\n---\n\nplain text"} />,
    );
    expect(container.querySelector("blockquote")).not.toBeNull();
    expect(container.querySelector("hr")).not.toBeNull();
    expect(screen.getByText("plain text")).toBeDefined();
  });
});
