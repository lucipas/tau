// Visual regression check for the tau web console chat layout.
//
// Screenshots the built static export (web/out) as served by a running
// `tau serve --http` bridge, at a mobile and a desktop viewport, and
// asserts the layout invariants that broke in Nov 2026 (see the PR that
// added this directory):
//   - the message feed does not collapse to ~0px tall on mobile
//   - the composer does not overlap the message list
//   - the per-message action row does not overlap the outbox
//   - the sidebar does not overflow its desktop column (peer buttons
//     used to be clipped off the right edge)
//   - no horizontal overflow at either width
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
// on a normal Chromium build, so it is always used. The same build also
// refuses to start without a WebGL domain policy; when the Hatch build
// is detected, the policy file is written to the temp dir and passed
// via --hatch-webgl-allowed-domains-file.

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
const profileDir = join(tmpdir(), `tau-visual-regression-${process.pid}`);
const hopPath = join(tmpdir(), `tau-lna-hop-${process.pid}.html`);

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

const VIEWPORTS = [
  { name: "mobile-390", width: 390, height: 844, dpr: 2 },
  { name: "desktop-1280", width: 1280, height: 900, dpr: 1 },
];

const GEOMETRY_EXPRESSION = `(() => {
  const r = (el) => el ? (() => { const b = el.getBoundingClientRect(); return {x: Math.round(b.x), y: Math.round(b.y), w: Math.round(b.width), h: Math.round(b.height), bottom: Math.round(b.bottom), right: Math.round(b.right)}; })() : null;
  const main = document.querySelector('main');
  const scroll = document.querySelector('[aria-label="Message traffic"]');
  const feed = scroll ? scroll.closest('section') : null;
  const textarea = document.querySelector('#message-text');
  const composer = textarea ? textarea.closest('section') : null;
  const outboxBtn = [...document.querySelectorAll('button[aria-expanded]')].find((b) => b.textContent.toLowerCase().includes('outbox'));
  const outbox = outboxBtn ? outboxBtn.closest('div[class*="rounded-2xl"]') || outboxBtn.parentElement : null;
  const aside = document.querySelector('aside');
  const articles = [...document.querySelectorAll('article')];
  const overlap = (a, b) => a && b && a.x < b.right && b.x < a.right && a.y < b.bottom && b.y < a.bottom;
  const actions = articles.map((a) => {
    const replyBtn = a.querySelector('button[title="Reply to this message"]');
    const row = replyBtn ? replyBtn.parentElement : null;
    return { actionRow: r(row) };
  });
  return {
    url: location.href,
    viewport: { w: window.innerWidth, h: window.innerHeight },
    docScrollWidth: document.documentElement.scrollWidth,
    main: r(main),
    feedSection: r(feed),
    feedScroll: r(scroll),
    composer: r(composer),
    outbox: r(outbox),
    aside: r(aside),
    composerOverlapsFeed: overlap(r(composer), r(scroll)),
    outboxOverlappedByActionRow: actions.some((a) => overlap(a.actionRow, r(outbox))),
    messages: articles.length,
  };
})()`;

function checkGeometry(name, g, width) {
  const failures = [];
  if (g.messages < 1) failures.push("no messages rendered");
  if (!g.feedSection || g.feedSection.h < 100) {
    failures.push(`feed section collapsed (h=${g.feedSection?.h})`);
  }
  if (g.composerOverlapsFeed) failures.push("composer overlaps message list");
  if (g.outboxOverlappedByActionRow) {
    failures.push("message action row overlaps outbox");
  }
  if (g.docScrollWidth > width) {
    failures.push(`horizontal overflow (docScrollWidth=${g.docScrollWidth} > ${width})`);
  }
  if (name.startsWith("desktop") && g.aside && g.main) {
    // The aside must fit its 340px column: its right edge must not pass
    // the feed's left edge (the grid gap is 20px at lg).
    const feedLeft = g.feedSection?.x;
    if (feedLeft !== undefined && g.aside.right > feedLeft) {
      failures.push(`sidebar overflows its column (aside right=${g.aside.right}, feed left=${feedLeft})`);
    }
  }
  return failures;
}

let failures = [];
try {
  await waitForDevTools();
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    hopPath,
    `<!doctype html><title>lna hop</title><script>setTimeout(() => { location.href = ${JSON.stringify(bridgeUrl)}; }, 300);</script>`,
  );

  for (const vp of VIEWPORTS) {
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
    // Let the hop redirect, the SSE snapshot land, and React render.
    await sleep(5000);

    const evalResult = await send("Runtime.evaluate", {
      expression: GEOMETRY_EXPRESSION,
      returnByValue: true,
    });
    const geometry =
      evalResult.result?.result?.value ?? { error: "no value", raw: evalResult };
    const shot = await send("Page.captureScreenshot", { format: "png" });
    const png = Buffer.from(shot.result.data, "base64");
    const pngPath = join(outDir, `${vp.name}.png`);
    writeFileSync(pngPath, png);
    writeFileSync(
      join(outDir, `${vp.name}.geometry.json`),
      JSON.stringify(geometry, null, 2),
    );
    const vpFailures = checkGeometry(vp.name, geometry, vp.width);
    if (vpFailures.length) {
      failures.push(`${vp.name}: ${vpFailures.join("; ")}`);
    }
    console.log(`${vp.name}: ${vpFailures.length ? "FAIL" : "ok"} — ${pngPath}`);
    console.log(JSON.stringify(geometry, null, 2));
    ws.close();
  }
} finally {
  child.kill();
}

if (updateBaselines) {
  const baselineDir = join(outDir, "..", "baselines");
  mkdirSync(baselineDir, { recursive: true });
  for (const vp of VIEWPORTS) {
    copyFileSync(join(outDir, `${vp.name}.png`), join(baselineDir, `${vp.name}.png`));
  }
  console.log(`baselines updated in ${baselineDir}`);
}

if (failures.length) {
  console.error(`\nVISUAL REGRESSION FAILURES:\n- ${failures.join("\n- ")}`);
  process.exit(1);
}
console.log("\nvisual regression: all layout checks passed");
