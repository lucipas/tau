import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import Avatar, { avatarColorIndex, initialsFor } from "./Avatar";

describe("initialsFor", () => {
  it("uses the first letters of the first two words", () => {
    expect(initialsFor("Alice Baker")).toBe("AB");
  });

  it("uses the first two letters of a single word", () => {
    expect(initialsFor("Bob")).toBe("BO");
  });

  it("falls back to the seed for unknown names", () => {
    expect(initialsFor("unknown", "a1b2c3d4")).toBe("A1");
    expect(initialsFor(null, "ff00")).toBe("FF");
  });
});

describe("avatarColorIndex", () => {
  it("is deterministic for the same seed", () => {
    expect(avatarColorIndex("a1b2c3d4")).toBe(avatarColorIndex("a1b2c3d4"));
  });
});

describe("Avatar", () => {
  it("renders initials", () => {
    render(<Avatar name="Alice Baker" seed="a1b2c3d4" />);
    expect(screen.getByText("AB")).toBeTruthy();
  });

  it("renders the same color class for the same seed", () => {
    const { container: a } = render(<Avatar name="Alice" seed="deadbeef" />);
    const { container: b } = render(<Avatar name="Alice" seed="deadbeef" />);
    expect(a.firstChild?.firstChild).toBeTruthy();
    expect((a.firstChild as Element).className).toBe((b.firstChild as Element).className);
  });
});
