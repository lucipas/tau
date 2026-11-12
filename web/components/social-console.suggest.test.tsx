import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanSuggestion } from "@/lib/types";

import { SuggestionList } from "./social-console";

function makeSuggestion(overrides: Partial<TanSuggestion> = {}): TanSuggestion {
  return {
    author: "e".repeat(64),
    name: "carol",
    posts: 2,
    likes: 3,
    reason: "3 like(s)",
    last_post: 1002,
    ...overrides,
  };
}

describe("SuggestionList", () => {
  it("shows each suggestion with its reason and a follow button", () => {
    const onFollow = vi.fn();
    render(
      <SuggestionList
        suggestions={[makeSuggestion()]}
        onFollow={onFollow}
      />,
    );

    expect(screen.getByText("carol")).toBeTruthy();
    expect(screen.getByText("3 like(s)")).toBeTruthy();
    expect(screen.getByText(/2 post\(s\)/)).toBeTruthy();

    fireEvent.click(screen.getByText("follow"));
    expect(onFollow).toHaveBeenCalledTimes(1);
    expect(onFollow).toHaveBeenCalledWith("e".repeat(64));
  });

  it("says so instead of rendering an empty box when nothing is suggested", () => {
    render(<SuggestionList suggestions={[]} onFollow={() => {}} />);

    expect(
      screen.getByText(
        "No suggestions yet. Follow an author or sync more posts first.",
      ),
    ).toBeTruthy();
    expect(screen.queryByText("follow")).toBeNull();
  });

  it("falls back to unknown for a suggestion without a name", () => {
    render(
      <SuggestionList
        suggestions={[makeSuggestion({ name: "" })]}
        onFollow={() => {}}
      />,
    );

    expect(screen.getByText("unknown")).toBeTruthy();
  });
});
