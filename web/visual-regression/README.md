# Visual regression check for the rewritten (v2) tau web console

Screenshots the built static export as served by a running
`tau serve --http` bridge, at a mobile (390px) and a desktop (1280px)
viewport, for two views — the Social feed (`#social`) and the seeded
bob chat thread (`#chat?peer=<bob destination>`) — and asserts the
layout invariants:

- no horizontal overflow at either width
- the section nav is present (left nav on desktop, bottom tab bar on
  mobile) and does not overlap the center column
- Social: the Tan composer sits at the top of the feed in normal flow
  and the seeded post renders (article count >= 1)
- Chat: the message thread renders tall enough to show messages, the
  composer sits below the thread in normal flow (no overlap), the
  conversation list keeps its column on desktop (no clipping)

## Run it

```sh
web/visual-regression/run.sh            # builds, seeds, screenshots, checks
SKIP_BUILD=1 web/visual-regression/run.sh   # skip the `make web-build` step
web/visual-regression/run.sh /tmp/shots --update-baselines
```

`run.sh` starts a fresh two-node demo (alice serves the console on
http://127.0.0.1:8787 with `--social`, bob is the peer), seeds one
inbound and one outbound DM plus a Tan post from bob (alice follows
bob so it shows on the default Following feed), runs
`screenshot.mjs`, then stops both nodes. It exits non-zero and prints
the failing invariant when a check fails. Screenshots and geometry
JSON land in `out/` (default: `web/visual-regression/out/`).

`--update-baselines` copies the freshly captured screenshots into
`baselines/` — only do this after eyeballing them and confirming the
layout is good.

## How it works

- `run.sh` — builds the export, starts the demo nodes, seeds state.
- `seed.py` — waits for alice's bridge, sends bob → alice over the
  wire (CLI send, recorded as inbound on alice) and alice → bob via
  POST /api/messages (recorded as outbound on alice), publishes a Tan
  post from bob, then has alice follow bob so the post lands on the
  default Following feed.
- `screenshot.mjs` — drives headless Chromium over CDP with Node's
  built-in WebSocket client (no npm deps). It navigates via a
  `file://` hop page, waits for the snapshot + React render, sets the
  view hash, evaluates bounding-box geometry, captures a PNG per
  viewport × view, and fails when an invariant breaks.

The check is geometry assertions (`getBoundingClientRect`), not pixel
diffs: layout overlap is what broke last time, and rects are
deterministic where pixels are not. The `baselines/` PNGs are for
eyeball comparison in review, not for automated diffing.

## Chromium notes

Chromium is located via `CHROME_BIN`, then `chromium`,
`chromium-browser`, `google-chrome`, `google-chrome-stable`, then
`/opt/meta-chromium/chrome` (the Hatch CVM build).

The Hatch CVM Chromium build force-enables Local Network Access
checks, which block navigations to loopback addresses. The script
works around it by navigating through a `file://` hop page first (a
local → local navigation, which is allowed). The hop is harmless on a
normal Chromium build, so it is always used. The same build also
refuses to start without a WebGL domain policy; when the Hatch build
is detected, the policy file is written to the temp dir and passed
via `--hatch-webgl-allowed-domains-file`.
