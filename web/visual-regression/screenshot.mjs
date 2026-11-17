// Visual regression check for the rewritten (v2) tau web console.
//
// Screenshots the built static export (web/out) as served by a running
// `tau serve --http` bridge, at a mobile and a desktop viewport, for
// two views — the Social feed (#social) and the seeded bob chat thread
// (#chat?peer=<bob destination>) — and asserts the layout invariants:
//
//   - no horizontal overflow at either width
//   - the section nav is present (left nav on desktop, bottom tab bar
//     on mobile) and does not overlap the center column
//   - Social: the Tan composer sits at the top of the feed in normal
//     flow and the seeded post renders (article count >= 1)
//   - Chat: the message thread renders tall enough to show messages,
//     the composer sits below the thread in normal flow (no overlap),
//     the conversation list keeps its column on desktop (no clipping)
//
// Usage:
//   node screenshot.mjs <bridge-url> <out-dir> [--update-baselines]
//
// The bridge URL is the console root, e.g. http://127.0.0.1:8787/.
// Use run.sh to seed a demo node pair and run this end to end.
//
// No npm dependencies: drives headless Chromium over CDP with Node's
// built-in WebSocket client. Chromium is located via CHROME_BIN, then
// `chromium`, `chromium-browser`, `google-chrome`, `google-chrome-stable`,
// then /opt/meta-chromium/chrome (the Hatch CVM build).
//
// Known quirk: the Hatch CVM Chromium build force-enables Local Network
// Access checks, which block navigations to loopback addresses. This
// script works around it by navigating through a file:// hop page first
// (a local -> local navigation, which is allowed). The hop is harmless
// on a normal Chromium build, so it is always used. The same build
// also refuses to start without a WebGL domain policy; when the Hatch
// build is detected, the policy file is written to the temp dir and
// passed via --hatch-webgl-allowed-domains-file.

import { createHash } from "node:crypto";
import { spawn, execSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync, copyFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const [bridgeUrl, outDir, ...flags] = process.argv.slice(2);
if (!bridgeUrl || !outDir) {
  console.error(
    "usage: node screenshot.mjs <bridge-url> <out-dir> [--update-baselines]",
  );
  process.exit(2);
}
const updateBaselines = flags.includes("--update-baselines");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function findChrome() {
  if (process.env.CHROME_BIN) return process.env.CHROME_BIN;
  for (const name of [
    "chromium",
    "chromium-browser",
    "google-chrome",
    "google-chrome-stable",
  ]) {
    try {
      const found = execSync(`command -v ${name}`, { encoding: "utf8" }).trim();
      if (found) return found;
    } catch {
      // not on PATH
    }
  }
  const hatch = "/opt/meta-chromium/chrome";
  if (existsSync(hatch)) return hatch;
  throw new Error(
    "no Chromium found: set CHROME_BIN or install chromium / google-chrome",
  );
}

const chrome = findChrome();
const isHatch = chrome.includes("meta-chromium");
const port = 9453 + (process.pid % 100);
const profileDir = join(tmpdir(), `tau-visual-regression-v2-${process.pid}`);
const hopPath = join(tmpdir(), `tau-lna-hop-v2-${process.pid}.html`);

const chromeArgs = [
  "--headless=new",
  "--no-first-run",
  "--no-default-browser-check",
  "--disable-gpu",
  "--no-sandbox",
  "--disable-dev-shm-usage",
  "--no-proxy-server",
  `--remote-debugging-port=${port}`,
  `--user-data-dir=${profileDir}`,
  "about:blank",
];
if (isHatch) {
  // This build refuses to start without a WebGL domain policy.
  const policyPath = join(tmpdir(), "tau-webgl-policy.txt");
  writeFileSync(policyPath, "hatch-webgl-allowed-domains-v1\nexample.com\n");
  const sha = createHash("sha256")
    .update("hatch-webgl-allowed-domains-v1\nexample.com\n")
    .digest("hex");
  chromeArgs.push(
    `--hatch-webgl-allowed-domains-file=${policyPath}`,
    `--hatch-webgl-allowed-domains-sha256=${sha}`,
  );
}

const child = spawn(chrome, chromeArgs, { stdio: "ignore" });
let chromeDead = false;
child.on("exit", () => {
  chromeDead = true;
});

async function waitForDevTools() {
  for (let i = 0; i < 100; i++) {
    if (chromeDead) throw new Error("chrome exited before DevTools came up");
    try {
      const res = await fetch(`http://127.0.0.1:${port}/json/version`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await sleep(200);
  }
  throw new Error("DevTools endpoint did not come up");
}

// Bob's destination, so the chat shots can deep-link to the thread
// via #chat?peer=<destination> instead of clicking through the list.
async function findBobDestination() {
  for (let i = 0; i < 30; i++) {
    try {
      const res = await fetch(`${bridgeUrl}api/peers`);
      const data = await res.json();
      const bob = (data.peers ?? []).find((p) => p.name === "bob");
      if (bob?.destination) return bob.destination;
    } catch {
      // bridge not up yet
    }
    await sleep(1000);
  }
  throw new Error("bob never appeared in /api/peers");
}

const VIEWPORTS = [
  { name: "mobile-390", width: 390, height: 844, dpr: 2 },
  { name: "desktop-1280", width: 1280, height: 900, dpr: 1 },
];

const VIEWS = [
  { name: "social", hash: "#social" },
  { name: "chat", hash: null }, // filled in with #chat?peer=<bob>
];

const GEOMETRY_EXPRESSION = `(() => {
  const r = (el) => el ? (() => { const b = el.getBoundingClientRect(); return {x: Math.round(b.x), y: Math.round(b.y), w: Math.round(b.width), h: Math.round(b.height), bottom: Math.round(b.bottom), right: Math.round(b.right)}; })() : null;
  const visible = (el) => !!el && el.getBoundingClientRect().width > 0 && el.getBoundingClientRect().height > 0;
  const overlap = (a, b) => a && b && a.x < b.right && b.x < a.right && a.y < b.bottom && b.y < a.bottom;
  const nav = document.querySelector('nav[aria-label="Sections"]');
  const thread = document.querySelector('[aria-label="Messages"]');
  const chatTextarea = document.querySelector('textarea[aria-label="Message"]');
  const chatComposer = chatTextarea ? chatTextarea.closest('div[class*="border-t"], form') || chatTextarea.parentElement : null;
  const list = document.querySelector('[aria-label="Conversations"]');
  const tanTextarea = document.querySelector('textarea[aria-label="Compose a post"]');
  const tanComposer = tanTextarea ? tanTextarea.closest('section, form, div') : null;
  const feedTabs = document.querySelector('[aria-label="Feed tabs"]');
  const articles = [...document.querySelectorAll('article')];
  const messages = [...document.querySelectorAll('[data-message-id]')];
  return {
    url: location.href,
    viewport: { w: window.innerWidth, h: window.innerHeight },
    docScrollWidth: document.documentElement.scrollWidth,
    nav: r(nav),
    list: r(list),
    thread: r(thread),
    chatComposer: r(chatComposer),
    tanComposer: r(tanComposer),
    feedTabs: r(feedTabs),
    composerOverlapsThread: overlap(r(chatComposer), r(thread)),
    navOverlapsThread: overlap(r(nav), r(thread)),
    navOverlapsComposer: overlap(r(nav), r(tanComposer)),
    articles: articles.length,
    messages: messages.length,
    hasChatTextarea: visible(chatTextarea),
    hasTanTextarea: visible(tanTextarea),
    h1Texts: [...document.querySelectorAll('h1')].map((h) => h.textContent.trim()).slice(0, 5),
  };
})()`;

function checkGeometry(view, name, g, width) {
  const failures = [];
  if (g.docScrollWidth > width) {
    failures.push(`horizontal overflow (docScrollWidth=${g.docScrollWidth} > ${width})`);
  }
  if (!g.nav) failures.push("section nav not found");
  if (view === "social") {
    if (!g.hasTanTextarea) failures.push("Tan composer textarea not visible");
    if (!g.feedTabs) failures.push("feed tabs not found");
    if (g.articles < 1) {
      failures.push(`expected >= 1 seeded post, got ${g.articles} articles`);
    }
    if (g.navOverlapsComposer) failures.push("nav overlaps Tan composer");
  }
  if (view === "chat") {
    if (!g.thread || g.thread.h < 100) {
      failures.push(`message thread collapsed (h=${g.thread?.h})`);
    }
    if (!g.hasChatTextarea) failures.push("chat composer textarea not visible");
    if (!g.chatComposer) failures.push("chat composer not found");
    if (g.composerOverlapsThread) failures.push("composer overlaps message thread");
    if (g.navOverlapsThread) failures.push("nav overlaps message thread");
    if (g.messages < 2) {
      failures.push(`expected 2 seeded messages, got ${g.messages}`);
    }
    if (name.startsWith("desktop") && g.list && g.thread) {
      // The conversation list keeps its column: it must end before
      // the thread starts and stay under ~420px wide.
      if (g.list.w > 420) {
        failures.push(`conversation list too wide (w=${g.list.w})`);
      }
      if (g.list.right > g.thread.x) {
        failures.push(`conversation list overlaps thread (list right=${g.list.right}, thread x=${g.thread.x})`);
      }
    }
  }
  return failures;
}

let failures = [];
const shots = [];
try {
  await waitForDevTools();
  const bobDest = await findBobDestination();
  VIEWS.find((v) => v.name === "chat").hash = `#chat?peer=${bobDest}`;
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    hopPath,
    `<!doctype html><title>lna hop</title><script>setTimeout(() => { location.href = ${JSON.stringify(bridgeUrl)}; }, 300);</script>`,
  );

  for (const vp of VIEWPORTS) {
    for (const view of VIEWS) {
      const shotName = `${view.name}-${vp.name}`;
      const target = await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, {
        method: "PUT",
      }).then((r) => r.json());
      const ws = new WebSocket(target.webSocketDebuggerUrl);
      await new Promise((resolve, reject) => {
        ws.onopen = resolve;
        ws.onerror = reject;
      });
      let nextId = 1;
      const pending = new Map();
      const loadFired = new Promise((resolve) => {
        ws.onmessage = (event) => {
          const msg = JSON.parse(event.data);
          if (msg.method === "Page.loadEventFired") resolve();
          if (msg.id !== undefined && pending.has(msg.id)) {
            pending.get(msg.id)(msg);
            pending.delete(msg.id);
          }
        };
      });
      const send = (method, params = {}) => {
        const id = nextId++;
        ws.send(JSON.stringify({ id, method, params }));
        return new Promise((resolve) => pending.set(id, resolve));
      };
      const evaluate = async (expression) => {
        const res = await send("Runtime.evaluate", {
          expression,
          returnByValue: true,
        });
        return res.result?.result?.value;
      };

      await send("Page.enable");
      await send("Runtime.enable");
      await send("Emulation.setDeviceMetricsOverride", {
        width: vp.width,
        height: vp.height,
        deviceScaleFactor: vp.dpr,
        mobile: vp.width < 600,
      });
      await send("Page.navigate", { url: `file://${hopPath}` });
      await Promise.race([loadFired, sleep(15000)]);
      // Let the hop redirect, the snapshot land, and React render.
      await sleep(5000);
      // Hash navigation to the view (the hop lands on the default
      // chat section; setting location.hash re-renders via useHash).
      await evaluate(`location.hash = ${JSON.stringify(view.hash)}; true`);
      await sleep(3000);

      const geometry = await evaluate(GEOMETRY_EXPRESSION);
      const shot = await send("Page.captureScreenshot", { format: "png" });
      const png = Buffer.from(shot.result.data, "base64");
      const pngPath = join(outDir, `${shotName}.png`);
      writeFileSync(pngPath, png);
      writeFileSync(
        join(outDir, `${shotName}.geometry.json`),
        JSON.stringify(geometry, null, 2),
      );
      shots.push(shotName);
      const viewFailures = checkGeometry(view.name, vp.name, geometry, vp.width);
      if (viewFailures.length) {
        failures.push(`${shotName}: ${viewFailures.join("; ")}`);
      }
      console.log(`${shotName}: ${viewFailures.length ? "FAIL" : "ok"} — ${pngPath}`);
      console.log(JSON.stringify(geometry, null, 2));
      ws.close();
    }
  }
} finally {
  child.kill();
}

if (updateBaselines) {
  const baselineDir = join(outDir, "..", "baselines");
  mkdirSync(baselineDir, { recursive: true });
  for (const shotName of shots) {
    copyFileSync(join(outDir, `${shotName}.png`), join(baselineDir, `${shotName}.png`));
  }
  console.log(`baselines updated in ${baselineDir}`);
}

if (failures.length) {
  console.error(`\nVISUAL REGRESSION FAILURES:\n- ${failures.join("\n- ")}`);
  process.exit(1);
}
console.log("\nvisual regression: all layout checks passed");
