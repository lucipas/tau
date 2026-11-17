// Pages tab for the rewritten console: personal mesh pages cached
// from peers, plus this node's own page. A page is a signed Markdown
// document — a notice board, a meetup page, a file index — fetched
// over RNS on demand. Rendering is safe by construction: the Markdown
// renderer builds React elements only and never injects HTML.
"use client";

import { useState } from "react";

import { apiDownload } from "@/lib/api";
import { formatDateTime, formatFileSize, shortHash } from "@/lib/format";
import type { MeshPage, MeshPageFile } from "@/lib/types";
import type { usePages } from "@/lib/use-pages";
import type { UseTauResult } from "@/lib/use-tau";

import { Markdown } from "./markdown";
import { Button, Card, CardTitle, Chip, EmptyState, ErrorNote } from "./ui";

export function PagesTab({
  tau,
  pages,
}: {
  tau: UseTauResult;
  pages: ReturnType<typeof usePages>;
}) {
  const [fetchTarget, setFetchTarget] = useState("");
  const [downloading, setDownloading] = useState<string | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  function authorName(author: string): string {
    if (pages.mine !== null && pages.mine.author === author) {
      return "your page";
    }
    return (
      tau.peers.find((peer) => peer.destination === author)?.name ||
      shortHash(author)
    );
  }

  async function submitFetch(event: React.FormEvent) {
    event.preventDefault();
    const target = fetchTarget.trim();
    if (target === "" || pages.fetching) return;
    const fetched = await pages.fetchPage(target);
    if (fetched !== null) setFetchTarget("");
  }

  async function downloadFile(page: MeshPage, file: MeshPageFile) {
    setDownloading(file.sha256);
    setDownloadError(null);
    try {
      const { blob, filename } = await apiDownload(
        `/api/pages/${page.author}/files/${file.sha256}`,
      );
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    } catch (caught) {
      setDownloadError(
        caught instanceof Error ? caught.message : "download failed",
      );
    } finally {
      setDownloading(null);
    }
  }

  const selected = pages.selectedPage;
  const entries: { page: MeshPage; mine: boolean }[] = [
    ...(pages.mine !== null ? [{ page: pages.mine, mine: true }] : []),
    ...pages.pages.map((page) => ({ page, mine: false })),
  ];

  return (
    <div data-testid="pages-tab" className="flex min-w-0 flex-col gap-4">
      <Card>
        <CardTitle
          right={
            <Button variant="subtle" onClick={() => void pages.refresh()}>
              Refresh
            </Button>
          }
        >
          Pages
        </CardTitle>
        <div className="flex min-w-0 flex-col gap-3 px-3 py-3">
          <p className="text-sm break-words text-mist">
            Personal mesh pages: a signed Markdown page a peer publishes
            about anything — a meetup, a project status, a file index.
            Fetch one by peer name or destination hash; only the newest
            signed version per author is kept.
          </p>
          <form onSubmit={submitFetch} className="flex min-w-0 gap-2">
            <label htmlFor="page-fetch" className="sr-only">
              Fetch a page from a peer
            </label>
            <input
              id="page-fetch"
              value={fetchTarget}
              onChange={(event) => setFetchTarget(event.target.value)}
              placeholder="peer name or destination hash"
              autoComplete="off"
              className="min-h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 text-sm focus:border-signal/60 focus:outline-none"
            />
            <Button
              type="submit"
              variant="primary"
              disabled={fetchTarget.trim() === "" || pages.fetching}
            >
              {pages.fetching ? "Fetching…" : "Fetch"}
            </Button>
          </form>
          <ErrorNote message={pages.fetchError} />
          {pages.fetchNote !== null ? (
            <p className="text-sm text-mist">{pages.fetchNote}</p>
          ) : null}
          {pages.loading ? (
            <EmptyState>Loading pages…</EmptyState>
          ) : entries.length === 0 ? (
            <EmptyState>
              No pages yet — fetch one from a peer above, or publish your
              own with `tau page publish`.
            </EmptyState>
          ) : (
            <ul className="flex min-w-0 flex-col gap-1">
              {entries.map(({ page, mine }) => {
                const active = page.author === pages.selected;
                return (
                  <li key={page.author}>
                    <button
                      type="button"
                      onClick={() => pages.select(page.author)}
                      aria-current={active}
                      className={`flex min-h-9 w-full min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border px-2.5 py-2 text-left text-sm ${
                        active
                          ? "border-signal/50 bg-signal/10 text-signal"
                          : "border-transparent text-mist hover:border-line hover:text-ink"
                      }`}
                    >
                      <span className="min-w-0 truncate">
                        {mine ? "Your page — " : ""}
                        {page.title || "Untitled page"}
                      </span>
                      <span className="flex shrink-0 flex-wrap items-center gap-2 text-xs">
                        {mine ? <Chip>own page</Chip> : null}
                        <span>
                          {authorName(page.author)} ·{" "}
                          {formatDateTime(page.updated_at)}
                        </span>
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
          <ErrorNote message={pages.error} />
        </div>
      </Card>

      {selected !== null ? (
        <Card>
          <CardTitle>{selected.title || "Untitled page"}</CardTitle>
          <div className="flex min-w-0 flex-col gap-3 px-3 py-3">
            <p className="text-xs break-words text-mist">
              <span className="text-sm text-ink">
                {authorName(selected.author)}
              </span>{" "}
              <span className="font-mono" title={selected.author}>
                {shortHash(selected.author)}
              </span>{" "}
              · v{selected.version} · {formatDateTime(selected.updated_at)}
              {pages.mine !== null &&
              pages.mine.author === selected.author ? (
                <span className="ml-2">
                  <Chip>own page</Chip>
                </span>
              ) : null}
            </p>
            <Markdown text={selected.markdown} />
            {selected.files.length > 0 ? (
              <div className="border-t border-line-soft pt-3">
                <h3 className="text-xs font-semibold tracking-widest text-mist uppercase">
                  Linked files
                </h3>
                <ul className="mt-2 flex min-w-0 flex-col gap-1">
                  {selected.files.map((file) => (
                    <li key={file.sha256}>
                      <button
                        type="button"
                        onClick={() => void downloadFile(selected, file)}
                        disabled={downloading === file.sha256}
                        className="flex min-h-9 w-full min-w-0 items-center justify-between gap-2 rounded-lg border border-transparent px-2.5 py-2 text-left text-sm text-mist hover:border-line hover:text-ink disabled:opacity-50"
                      >
                        <span className="min-w-0 truncate">{file.name}</span>
                        <span className="shrink-0 text-xs">
                          {formatFileSize(file.size)}
                          {downloading === file.sha256
                            ? " · downloading…"
                            : ""}
                        </span>
                      </button>
                    </li>
                  ))}
                </ul>
                <ErrorNote message={downloadError} />
              </div>
            ) : null}
          </div>
        </Card>
      ) : null}
    </div>
  );
}

export default PagesTab;
