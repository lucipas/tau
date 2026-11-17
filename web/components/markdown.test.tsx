import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Markdown } from "./markdown";

describe("Markdown", () => {
  it("renders headings, emphasis, code and links", () => {
    const { container } = render(
      <Markdown
        text={
          "# Meetup\n\nCome **Saturday** at *2pm*, bring `notes`.\n\nSee [the park](https://example.org/park) for directions.\n"
        }
      />,
    );

    expect(screen.getByRole("heading", { name: "Meetup" })).toBeTruthy();
    expect(container.querySelector("strong")?.textContent).toBe("Saturday");
    expect(container.querySelector("em")?.textContent).toBe("2pm");
    expect(container.querySelector("code")?.textContent).toBe("notes");
    const link = screen.getByRole("link", { name: "the park" });
    expect(link.getAttribute("href")).toBe("https://example.org/park");
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel")).toBe("noreferrer");
  });

  it("renders lists, quotes, rules and fenced code", () => {
    const { container } = render(
      <Markdown
        text={
          "- one\n- two\n\n1. first\n2. second\n\n> quoted line\n\n---\n\n```\nplain code\n```\n"
        }
      />,
    );

    expect(screen.getByText("one")).toBeTruthy();
    expect(screen.getByText("first")).toBeTruthy();
    expect(screen.getByText("quoted line")).toBeTruthy();
    expect(screen.getByText("plain code")).toBeTruthy();
    expect(container.querySelector("hr")).not.toBeNull();
    expect(container.querySelector("ul")).not.toBeNull();
    expect(container.querySelector("ol")).not.toBeNull();
    expect(container.querySelector("blockquote")).not.toBeNull();
  });

  it("never injects HTML from the page", () => {
    const { container } = render(
      <Markdown
        text={
          "<script>alert(1)</script>\n\n<img src=x onerror=alert(2)>\n"
        }
      />,
    );

    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    expect(screen.getByText("<script>alert(1)</script>")).toBeTruthy();
  });

  it("renders non-http links as plain text", () => {
    const { container } = render(
      <Markdown text="[click me](javascript:alert(1)) and [file](data:text/html;base64,xxx)" />,
    );

    expect(screen.queryByRole("link")).toBeNull();
    expect(container.querySelector("p")?.textContent).toBe(
      "[click me](javascript:alert(1)) and [file](data:text/html;base64,xxx)",
    );
  });
});
