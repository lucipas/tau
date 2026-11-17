# tau web console (v2 rewrite)

Next.js web UI for a running tau node. The console is a static export
(`output: "export"`) served by the tau bridge itself from `web/out` on the
same origin as the API — no separate server.

## Run

```sh
cd web
npm ci --no-audit
npm run dev        # http://localhost:3000, API proxied to the bridge on :8787
npm test           # vitest, lib layer only
npm run typecheck  # tsc --noEmit
npm run build      # static export into web/out (never committed)
```

`npm run dev` talks to a bridge on `http://localhost:8787` when the page
runs on localhost:3000/3001. Override with `NEXT_PUBLIC_API_BASE`, e.g.
`NEXT_PUBLIC_API_BASE=http://127.0.0.1:8787 npm run dev`.

## Layout

- `lib/api.ts` — typed fetch client for every bridge route (`tau/webapi.py`).
- `lib/types.ts` — all bridge JSON shapes.
- `lib/format.ts` — time / bytes / address formatting.
- `lib/peer-lookalike.ts` — client-side impersonation (lookalike) detection.
- `lib/use-tau.ts` — live snapshot via `/ws` + REST fallback, conversations,
  outbox, traffic, peer actions.
- `lib/use-tan.ts` — Tan feed, thread, composer, lists.
- `lib/use-channels.ts` — public channels.
- `lib/use-pages.ts` — personal mesh pages.
- `lib/use-now.ts` — clock tick for relative timestamps.

## Notes

- There is no `/api/traffic` endpoint: the traffic view is derived from
  the stored message history (`GET /api/messages`).
- Errors from the bridge are `{ "error": { "message": "..." } }` and surface
  as `ApiError` (with HTTP status) from every `api.ts` call.
- The bridge binds to loopback by default; export/import/backup endpoints
  are loopback-only and answer 403 from a remote client.
