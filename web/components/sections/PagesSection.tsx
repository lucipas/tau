// PagesSection.tsx — personal mesh pages: this node's own page, cached
// peer pages, a viewer (text + linked files), and fetch-a-peer's-page over
// RNS. Built on usePages (web/lib/use-pages.ts) + pageFileUrl.
//
// Page bodies are markdown; the viewer renders them as plain text
// paragraphs — no HTML injection, no markdown renderer.

"use client";

import { useState } from "react";
import Avatar from "@/components/chrome/Avatar";
import { pageFileUrl } from "@/lib/api";
import { formatBytes, formatDateTime, formatRelative, shortHash } from "@/lib/format";
import type { MeshPage } from "@/lib/types";
import { usePages } from "@/lib/use-pages";

/** Render markdown source as plain-text paragraphs (headings kept as text). */
export function renderPageText(markdown: string): { key: string; text: string; heading: boolean }[] {
  return markdown
    .split(/\n{2,}/)
    .map((block) => block.trim())
    .filter(Boolean)
    .map((block, i) => {
      const heading = block.startsWith("#");
      return {
        key: `block-${i}`,
        text: block.replace(/^#+\s*/, ""),
        heading,
      };
    });
}

export default function PagesSection() {
  const pages = usePages();
  const [selected, setSelected] = useState<MeshPage | null>(null);
  const [fetchPeer, setFetchPeer] = useState("");
  const [fetching, setFetching] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function handleFetch(e: React.FormEvent) {
    e.preventDefault();
    const peer = fetchPeer.trim();
    if (!peer) return setError("Type a peer destination hash to fetch.");
    setFetching(true);
    setError(null);
    setNotice(null);
    try {
      const page = await pages.fetch(peer);
      if (page) {
        setSelected(page);
        setFetchPeer("");
        setNotice(`Fetched ${page.title || "page"} from the mesh.`);
      } else {
        setNotice("No page came back — the peer may not publish one.");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setFetching(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-[600px]">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/95 px-3 py-3 backdrop-blur sm:px-4">
        <h1 className="text-lg font-bold text-zinc-50">
          <span className="mr-2 text-cyan-400">τ</span>Pages
        </h1>
        <p className="mt-0.5 text-xs text-zinc-500">
          Personal pages published on the mesh — yours, and the ones your peers share.
        </p>
      </header>

      {error && (
        <p role="alert" className="border-b border-zinc-800 px-4 py-2 text-sm text-red-400">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="border-b border-zinc-800 px-4 py-2 text-sm text-cyan-300">
          {notice}
        </p>
      )}

      {selected ? (
        <div>
          <div className="flex items-center gap-2 border-b border-zinc-800 px-3 py-2">
            <button
              type="button"
              onClick={() => setSelected(null)}
              className="rounded-full px-2 py-1 text-sm text-cyan-400 hover:bg-zinc-900"
            >
              ← All pages
            </button>
            <h2 className="min-w-0 truncate text-sm font-bold text-zinc-100">
              {selected.title || "Untitled page"}
            </h2>
          </div>
          <article className="px-3 py-4 sm:px-4">
            <div className="flex items-center gap-3">
              <Avatar name={shortHash(selected.author)} seed={selected.author} size="md" />
              <div className="min-w-0">
                <p className="truncate font-mono text-xs text-zinc-400">{selected.author}</p>
                <p className="text-xs text-zinc-500">
                  v{selected.version} · updated {formatDateTime(selected.updated_at)}
                </p>
              </div>
            </div>
            <div className="mt-4 flex flex-col gap-3">
              {renderPageText(selected.markdown).map((block) =>
                block.heading ? (
                  <h3 key={block.key} className="text-base font-bold text-zinc-100">
                    {block.text}
                  </h3>
                ) : (
                  <p key={block.key} className="text-sm leading-relaxed whitespace-pre-wrap text-zinc-200">
                    {block.text}
                  </p>
                ),
              )}
              {selected.markdown.trim() === "" && (
                <p className="text-sm text-zinc-500">This page has no text.</p>
              )}
            </div>
            {selected.files.length > 0 && (
              <div className="mt-6">
                <h3 className="text-sm font-bold text-zinc-100">Files</h3>
                <ul className="mt-2 flex flex-col gap-2">
                  {selected.files.map((f) => (
                    <li
                      key={f.sha256}
                      className="flex items-center justify-between gap-2 rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2"
                    >
                      <span className="min-w-0 truncate text-sm text-zinc-200">{f.name}</span>
                      <span className="flex shrink-0 items-center gap-2 text-xs text-zinc-500">
                        {f.size !== undefined && formatBytes(f.size)}
                        <a
                          href={pageFileUrl(selected.author, f.sha256)}
                          download={f.name}
                          className="rounded-full border border-zinc-700 px-2 py-0.5 font-semibold text-cyan-400 hover:bg-zinc-800"
                        >
                          Download
                        </a>
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </article>
        </div>
      ) : (
        <>
          <div className="border-b border-zinc-800 px-3 py-3 sm:px-4">
            <form onSubmit={handleFetch} className="flex gap-2">
              <input
                value={fetchPeer}
                onChange={(e) => setFetchPeer(e.target.value)}
                placeholder="Fetch a peer's page by destination hash…"
                aria-label="Peer destination hash to fetch"
                disabled={fetching}
                className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 font-mono text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
              />
              <button
                type="submit"
                disabled={fetching || !fetchPeer.trim()}
                className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
              >
                {fetching ? "Fetching…" : "Fetch"}
              </button>
            </form>
          </div>

          {pages.loading && (
            <p className="px-4 py-6 text-sm text-zinc-500">Loading pages…</p>
          )}
          {pages.error && (
            <p role="alert" className="px-4 py-6 text-sm text-red-400">
              {pages.error}
            </p>
          )}

          {pages.mine && (
            <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="My page">
              <h2 className="text-sm font-bold text-zinc-100">My page</h2>
              <PageRow page={pages.mine} onOpen={setSelected} mine />
            </section>
          )}

          <section className="px-3 py-4 sm:px-4" aria-label="Pages from the mesh">
            <h2 className="text-sm font-bold text-zinc-100">
              Pages from the mesh{" "}
              <span className="font-normal text-zinc-500">({pages.count})</span>
            </h2>
            {pages.pages.length === 0 && !pages.loading && (
              <p className="py-3 text-sm text-zinc-500">
                No peer pages cached yet — fetch one by destination hash above.
              </p>
            )}
            {pages.pages.map((p) => (
              <PageRow key={p.author} page={p} onOpen={setSelected} />
            ))}
            {pages.pages.length > 0 && (
              <button
                type="button"
                onClick={() => void pages.refresh()}
                className="mt-3 rounded-full border border-zinc-700 px-4 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
              >
                Refresh pages
              </button>
            )}
          </section>
        </>
      )}
    </div>
  );
}

function PageRow({
  page,
  onOpen,
  mine = false,
}: {
  page: MeshPage;
  onOpen: (page: MeshPage) => void;
  mine?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={() => onOpen(page)}
      className="mt-2 flex w-full items-center gap-3 rounded-xl border border-zinc-800 bg-zinc-900 px-3 py-3 text-left hover:border-zinc-600"
    >
      <Avatar name={page.title || shortHash(page.author)} seed={page.author} size="md" />
      <span className="min-w-0 flex-1">
        <span className="block truncate text-sm font-bold text-zinc-100">
          {page.title || "Untitled page"}
          {mine && (
            <span className="ml-2 rounded-full bg-zinc-800 px-2 py-0.5 text-[10px] font-semibold text-zinc-400">
              yours
            </span>
          )}
        </span>
        <span className="block truncate font-mono text-xs text-zinc-500">
          {page.author}
        </span>
        <span className="block text-xs text-zinc-500">
          updated {formatRelative(page.updated_at)}
          {page.files.length > 0 &&
            ` · ${page.files.length} file${page.files.length === 1 ? "" : "s"}`}
        </span>
      </span>
      <span aria-hidden="true" className="shrink-0 text-zinc-500">
        →
      </span>
    </button>
  );
}
