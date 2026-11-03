# tau console

Next.js + Tailwind frontend for a tau node. It is a static export
(`output: "export"`) so the Python bridge can serve it without a Node runtime.

```bash
npm install
npm run dev      # http://localhost:3000, expects the bridge on :8787
npm run build    # writes web/out, served by `tau serve --http`
```

The console reads `/api/status`, `/api/events` (SSE) and posts to
`/api/messages` and `/api/ping`. In development it automatically targets
`http://<host>:8787`; set `NEXT_PUBLIC_API_BASE` to override.

```bash
# Terminal 1: node + bridge
tau serve --http

# Terminal 2: hot-reload UI
npm run dev
```

Design tokens live in `app/globals.css` (`@theme`). Components are plain React
with no UI framework dependency.
