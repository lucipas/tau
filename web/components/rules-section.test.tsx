import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanRules } from "@/lib/types";

import { RulesSection } from "./rules-section";

const ALICE = "ab".repeat(16);
const BOB = "cd".repeat(16);

function makeRules(overrides: Partial<TanRules> = {}): TanRules {
  return {
    default: "allow",
    allow_authors: [],
    block_authors: [],
    allow_followed: false,
    kinds: null,
    ...overrides,
  };
}

function renderSection(
  rules: TanRules | null,
  options: {
    availableKinds?: string[];
    loading?: boolean;
    error?: string | null;
    onUpdate?: (next: TanRules) => Promise<TanRules>;
  } = {},
) {
  const onUpdate =
    options.onUpdate ?? vi.fn(async (next: TanRules) => next);
  const onRefresh = vi.fn(async () => {});
  render(
    <RulesSection
      rules={rules}
      availableKinds={options.availableKinds ?? ["post", "reply", "like"]}
      loading={options.loading ?? false}
      error={options.error ?? null}
      onUpdate={onUpdate}
      onRefresh={onRefresh}
    />,
  );
  return { onUpdate, onRefresh };
}

describe("RulesSection", () => {
  it("shows a loading state while the rules arrive", () => {
    renderSection(null, { loading: true });
    expect(screen.getByText("loading rules…")).toBeTruthy();
  });

  it("shows the load error when the rules never arrive", () => {
    renderSection(null, { loading: false, error: "node unreachable" });
    expect(screen.getByText("node unreachable")).toBeTruthy();
  });

  it("renders the current ruleset", () => {
    renderSection(
      makeRules({
        default: "deny",
        allow_authors: [ALICE],
        block_authors: [BOB],
        allow_followed: true,
      }),
    );
    expect(screen.getByText("deny all")).toBeTruthy();
    expect(screen.getByText(/Only allow-listed authors/)).toBeTruthy();
    const followed = screen.getByLabelText("admit authors you follow");
    expect((followed as HTMLInputElement).checked).toBe(true);
    expect(screen.getByTitle(ALICE)).toBeTruthy();
    expect(screen.getByTitle(BOB)).toBeTruthy();
  });

  it("applies the deny-all default at once", async () => {
    const { onUpdate } = renderSection(makeRules());
    fireEvent.click(screen.getByText("deny all"));
    await waitFor(() =>
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ default: "deny" }),
      ),
    );
  });

  it("toggles admitting followed authors", async () => {
    const { onUpdate } = renderSection(makeRules());
    fireEvent.click(screen.getByLabelText("admit authors you follow"));
    await waitFor(() =>
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ allow_followed: true }),
      ),
    );
  });

  it("unchecking a kind sends an explicit list of the rest", async () => {
    const { onUpdate } = renderSection(makeRules());
    fireEvent.click(screen.getByText("like"));
    await waitFor(() =>
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ kinds: ["post", "reply"] }),
      ),
    );
  });

  it("checking the last hidden kind clears the filter", async () => {
    const { onUpdate } = renderSection(
      makeRules({ kinds: ["post", "reply"] }),
    );
    fireEvent.click(screen.getByText("like"));
    await waitFor(() =>
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ kinds: null }),
      ),
    );
  });

  it("adds an author to the allow list", async () => {
    const { onUpdate } = renderSection(makeRules());
    const inputs = screen.getAllByPlaceholderText("name or author hash");
    fireEvent.change(inputs[0], { target: { value: "alice" } });
    fireEvent.click(screen.getAllByText("add")[0]);
    await waitFor(() =>
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ allow_authors: ["alice"] }),
      ),
    );
  });

  it("removes an author from the block list", async () => {
    const { onUpdate } = renderSection(
      makeRules({ block_authors: [BOB] }),
    );
    fireEvent.click(screen.getByTitle("remove"));
    await waitFor(() =>
      expect(onUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ block_authors: [] }),
      ),
    );
  });

  it("shows the save error and reverts when the server rejects a change", async () => {
    const onUpdate = vi.fn(async (_next: TanRules) => {
      throw new Error("unknown author or peer: 'nobody'");
    });
    renderSection(makeRules(), { onUpdate });
    fireEvent.click(screen.getByText("deny all"));
    await waitFor(() =>
      expect(
        screen.getByText("unknown author or peer: 'nobody'"),
      ).toBeTruthy(),
    );
    expect(onUpdate).toHaveBeenCalledTimes(1);
  });
});
