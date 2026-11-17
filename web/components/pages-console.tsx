"use client";

import { useState } from "react";

import { apiDownload } from "@/lib/api";
import { formatDateTime, formatFileSize, shortHash } from "@/lib/format";
import type { MeshPage, MeshPageFile, Peer } from "@/lib/types";
import { usePages } from "@/lib/use-pages";

import { Markdown } from "./markdown";
import { SectionLabel } from "./ui";

// The Pages view: personal mesh pages cached from peers, plus this
// node's own page. A page is a signed Markdown document — a notice
// board, a meetup page, a file index — fetched over RNS on demand,
// so nothing here is a live feed: the list refreshes when a page is
// fetched or the refresh button is pressed.
//
// Rendering is safe by construction: the Markdown renderer builds
// React elements only, never injects HTML, and only http(s) links
// become clickable.
export function PagesConsole({
  embedded = false,
  peers,
}: {
  embedded?: boolean;
  // Known peers, for resolving a page author to a name.
  peers: Peer[];
}) {
  const pages = usePages();
  const [fetchTarget, setFetchTarget] = useState("");
  const [downloading, setDownloading] = useState<string | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  function authorName(author: string): string {
    if (pages.mine !== null && pages.mine.author === author) {
      return "your page";
    }
    return peers.find((peer) => peer.destination === author)?.name ||
      shortHash(author);
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
    <div
      data-testid="pages-tab"
      className={
        embedded
          ? "flex min-h-0 flex-1 flex-col gap-4"
          : "flex min-h-0 flex-col gap-4"
      }
    >
      <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
        <SectionLabel>pages</SectionLabel>
        <p className="mt-2 text-[11px] leading-relaxed text-mist">
          Personal mesh pages: a signed Markdown page a peer publishes
          about anything — a meetup, a project status, a file index.
          Fetch one by peer name or destination hash; only the newest
          signed version per author is kept.
        </p>
        <form onSubmit={submitFetch} className="mt-3 flex items-center gap-2">
          <label htmlFor="page-fetch" className="sr-only">
            fetch a page from a peer
          </label>
          <input
            id="page-fetch"
            value={fetchTarget}
            onChange={(event) => setFetchTarget(event.target.value)}
            placeholder="peer name or destination hash"
            autoComplete="off"
            className="min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[12px] text-ink placeholder:text-mist/60 focus:border-signal/60 focus:outline-none"
          />
          <button
            type="submit"
            disabled={fetchTarget.trim() === "" || pages.fetching}
            className="shrink-0 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
          >
            {pages.fetching ? "fetching…" : "fetch"}
          </button>
        </form>
        {pages.fetchError !== null ? (
          <p className="mt-2 font-mono text-[11px] text-rose-300">
            {pages.fetchError}
          </p>
        ) : null}
        {pages.fetchNote !== null ? (
          <p className="mt-2 font-mono text-[11px] text-mist">
            {pages.fetchNote}
          </p>
        ) : null}
        {pages.loading ? (
          <p className="mt-3 font-mono text-[11px] text-mist">
            loading pages…
          </p>
        ) : entries.length === 0 ? (
          <p className="mt-3 font-mono text-[11px] text-mist">
            no pages yet — fetch one from a peer above, or publish your
            own with `tau page publish`.
          </p>
        ) : (
          <ul className="mt-3 flex flex-col gap-1">
            {entries.map(({ page, mine }) => {
              const active = page.author === pages.selected;
              return (
                <li key={page.author}>
                  <button
                    type="button"
                    onClick={() => pages.select(page.author)}
                    aria-current={active}
                    className={
                      active
                        ? "flex w-full items-center justify-between rounded-lg border border-signal/50 bg-signal/10 px-2.5 py-2 text-left font-mono text-[12px] text-signal"
                        : "flex w-full items-center justify-between rounded-lg border border-transparent px-2.5 py-2 text-left font-mono text-[12px] text-mist transition hover:border-line hover:text-ink"
                    }
                  >
                    <span className="min-w-0 truncate">
                      {mine ? "your page — " : ""}
                      {page.title || "Untitled page"}
                    </span>
                    <span className="shrink-0 text-[10px] opacity-70">
                      {authorName(page.author)} ·{" "}
                      {formatDateTime(page.updated_at)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
        {pages.error !== null ? (
          <p className="mt-2 font-mono text-[11px] text-rose-300">
            {pages.error}
          </p>
        ) : null}
      </section>

      {selected !== null ? (
        <section className="flex min-h-0 flex-1 flex-col rounded-2xl border border-line/80 bg-panel/40 p-4">
          <div className="flex items-center justify-between gap-2">
            <SectionLabel>
              {selected.title || "Untitled page"}
            </SectionLabel>
            <button
              type="button"
              onClick={() => void pages.refresh()}
              className="rounded-lg border border-line bg-void/70 px-2.5 py-1.5 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal"
            >
              refresh
            </button>
          </div>
          <p className="mt-2 font-mono text-[11px] text-mist">
            <span className="text-ink">{authorName(selected.author)}</span>{" "}
            <span title={selected.author}>
              {shortHash(selected.author)}
            </span>{" "}
            · v{selected.version} · {formatDateTime(selected.updated_at)}
          </p>
          <div className="thin-scroll mt-3 min-h-0 flex-1 overflow-y-auto">
            <Markdown text={selected.markdown} />
          </div>
          {selected.files.length > 0 ? (
            <div className="mt-4 border-t border-line/80 pt-3">
              <SectionLabel>linked files</SectionLabel>
              <ul className="mt-2 flex flex-col gap-1">
                {selected.files.map((file) => (
                  <li key={file.sha256}>
                    <button
                      type="button"
                      onClick={() => void downloadFile(selected, file)}
                      disabled={downloading === file.sha256}
                      className="flex w-full items-center justify-between rounded-lg border border-transparent px-2.5 py-2 text-left font-mono text-[12px] text-mist transition hover:border-line hover:text-ink disabled:opacity-50"
                    >
                      <span className="min-w-0 truncate">{file.name}</span>
                      <span className="shrink-0 text-[10px] opacity-70">
                        {formatFileSize(file.size)}
                        {downloading === file.sha256 ? " · downloading…" : ""}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
              {downloadError !== null ? (
                <p className="mt-2 font-mono text-[11px] text-rose-300">
                  {downloadError}
                </p>
              ) : null}
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
