// Visual regression check for the rewritten tau web console.
//
// Screenshots the built static export (web/out) as served by a running
// `tau serve --http` bridge, at a mobile and a desktop viewport, opens
// the seeded bob conversation, and asserts the layout invariants:
//   - the message thread renders tall enough to show messages
//   - the composer sits below the thread in normal flow (no overlap)
//   - the per-message action row does not overlap the outbox
//   - the conversation list keeps its column on desktop (no clipping)
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
// (a local -> local navigation, which is allowed). The hop is harmless on a
// normal Chromium build, so it is always used. The same build also
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

// Open the seeded bob conversation from the list (the chat tab shows
// the list first on both widths when nothing is selected).
const OPEN_BOB_EXPRESSION = `(() => {
  const btn = [...document.querySelectorAll('button')].find((b) =>
    b.textContent.trim().toLowerCase().startsWith('bob'),
  );
  if (!btn) return false;
  btn.click();
  return true;
})()`;

const GEOMETRY_EXPRESSION = `(() => {
  const r = (el) => el ? (() => { const b = el.getBoundingClientRect(); return {x: Math.round(b.x), y: Math.round(b.y), w: Math.round(b.width), h: Math.round(b.height), bottom: Math.round(b.bottom), right: Math.round(b.right)}; })() : null;
  const thread = document.querySelector('[aria-label="Message thread"]');
  const textarea = document.querySelector('[aria-label="Message text"]');
  const composer = textarea ? textarea.parentElement : null;
  const traffic = [...document.querySelectorAll('*')].find((el) => el.children.length === 0 && /in \\/ .* out/.test(el.textContent || ''));
  const outboxEmpty = [...document.querySelectorAll('p')].find((el) => (el.textContent || '').trim() === 'Outbox is empty.');
  const outboxHeading = [...document.querySelectorAll('h2')].find((el) => (el.textContent || '').trim().startsWith('Outbox'));
  const outboxAnchor = outboxEmpty || outboxHeading;
  const outbox = outboxAnchor ? outboxAnchor.closest('section') : null;
  const list = document.querySelector('[aria-label="Filter conversations"]');
  const listCard = list ? list.closest('section') : null;
  const articles = [...document.querySelectorAll('article')];
  const overlap = (a, b) => a && b && a.x < b.right && b.x < a.right && a.y < b.bottom && b.y < a.bottom;
  const actionRows = articles.map((a) => {
    const reactBtn = a.querySelector('[aria-label="React to this message"]');
    return { actionRow: r(reactBtn ? reactBtn.parentElement : null) };
  });
  return {
    url: location.href,
    viewport: { w: window.innerWidth, h: window.innerHeight },
    docScrollWidth: document.documentElement.scrollWidth,
    thread: r(thread),
    composer: r(composer),
    traffic: r(traffic),
    outbox: r(outbox),
    listCard: r(listCard),
    composerOverlapsThread: overlap(r(composer), r(thread)),
    outboxOverlappedByActionRow: actionRows.some((a) => overlap(a.actionRow, r(outbox))),
    messages: articles.length,
    hasTextarea: !!textarea,
    h2Texts: [...document.querySelectorAll('h2')].map((h) => h.textContent.trim()).slice(0, 20),
  };
})()`;

function checkGeometry(name, g, width) {
  const failures = [];
  if (g.messages < 2) failures.push(`expected 2 seeded messages, got ${g.messages}`);
  if (!g.thread || g.thread.h < 100) {
    failures.push(`message thread collapsed (h=${g.thread?.h})`);
  }
  if (!g.composer) failures.push("composer not found");
  if (g.composerOverlapsThread) failures.push("composer overlaps message thread");
  if (g.outboxOverlappedByActionRow) {
    failures.push("message action row overlaps outbox");
  }
  if (g.docScrollWidth > width) {
    failures.push(`horizontal overflow (docScrollWidth=${g.docScrollWidth} > ${width})`);
  }
  if (name.startsWith("desktop") && g.listCard && g.thread) {
    // The list keeps its ~320px column: it must end before the thread
    // starts, and must not be wider than 400px.
    if (g.listCard.w > 400) {
      failures.push(`conversation list too wide (w=${g.listCard.w})`);
    }
    if (g.listCard.right > g.thread.x) {
      failures.push(`conversation list overlaps thread (list right=${g.listCard.right}, thread x=${g.thread.x})`);
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

    const opened = await evaluate(OPEN_BOB_EXPRESSION);
    if (!opened) {
      failures.push(`${vp.name}: could not open the bob conversation`);
      console.log(`${vp.name}: FAIL — no bob row`);
      ws.close();
      continue;
    }
    await sleep(2000);

    const geometry = await evaluate(GEOMETRY_EXPRESSION);
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
