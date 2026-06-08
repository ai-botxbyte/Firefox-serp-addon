// BotXByte Firefox SERP Puller — background script
// =================================================
//
// Lifecycle:
//   1. Connect to local server.py via ws://localhost:8765
//   2. Toolbar icon = green when connected, grey when not. Auto-reconnect.
//   3. server.py sends {action:"runSerpBatch", id, jobs:[...]} (1..100 jobs).
//      Each job: {execution_id, domain_name, engine, query_type, queue_name, raw}
//   4. Group jobs by engine (google|bing) × query_type (index|news).
//      Open ONE tab per group:
//        - google/index : https://www.google.com/?q=hello
//        - bing/index   : https://www.bing.com/?q=hello
//        - google/news  : https://news.google.com/search?q=hello
//        - bing/news    : https://www.bing.com/news/search?q=hello
//      Wait for page load. If a Cloudflare Turnstile or reCAPTCHA challenge
//      is present, the existing turnstile_injected/turnstile_bridge content
//      scripts auto-click the checkbox and `clickAndSolveCaptcha` invokes a
//      Buster-style audio solve as a fallback.
//   5. Inside the loaded SERP tab, fire all 100 SERP queries in parallel via
//      `fetch()` from the page context (so they share cookies/session). Each
//      response is HTML — parse to extract result hostnames and decide
//      indexed/found.
//   6. Close the tab. Reply to server.py with {id, results:[...]}.
//
// IMPORTANT: this file is intentionally self-contained. Logging is verbose
// for the first iteration; tighten once the flow is verified end-to-end.

const WS_URL = 'ws://127.0.0.1:8765';
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const SERP_TAB_TIMEOUT_MS = 90_000;       // hard cap per group tab
const SERP_QUERY_TIMEOUT_MS = 30_000;      // per-query timeout (informational)
const TURNSTILE_WAIT_MS = 90_000;          // grace period waiting for solve (Buster audio takes ~30-60s)
const MAX_BATCH_RETRIES = 7;               // per-batch attempts: if all queries are blocked
                                           // (captcha/429/no content), re-warm (solve captcha)
                                           // and retry the whole 100-domain batch up to 7×.

const RT = (typeof browser !== 'undefined' && browser.runtime) ? browser : chrome;

// --- Proxy authentication ----------------------------------------------------
// Firefox's proxy is set directly to the rotating proxy (p.webshare.io:80) via
// the profile prefs, but Firefox can't carry proxy credentials in prefs — it
// would pop up a login dialog and freeze the automated session. So we supply
// the username/password automatically whenever the proxy challenges (407).
// Credentials come from proxy-config.js (self.PROXY_AUTH).
try {
  if (self.PROXY_AUTH && self.PROXY_AUTH.username && RT.webRequest && RT.webRequest.onAuthRequired) {
    RT.webRequest.onAuthRequired.addListener(
      (details) => {
        if (details && details.isProxy) {
          console.log('[proxy] supplying credentials for proxy auth challenge');
          return { authCredentials: { username: self.PROXY_AUTH.username, password: self.PROXY_AUTH.password } };
        }
        return {}; // not a proxy challenge — leave site auth alone
      },
      { urls: ['<all_urls>'] },
      ['blocking']
    );
    console.log(`[proxy] onAuthRequired handler registered for ${self.PROXY_AUTH.host}:${self.PROXY_AUTH.port}`);
  } else {
    console.log('[proxy] no proxy credentials configured (no-auth proxy or direct) — onAuthRequired not registered');
  }
} catch (e) {
  console.warn('[proxy] could not register onAuthRequired handler:', e && e.message);
}

let ws = null;
let wsReconnectDelay = RECONNECT_BASE_MS;
let wsConnected = false;
// Number of in-flight batches. The keepalive alarm sends a WS heartbeat
// while this is > 0 so MV3 won't suspend the background script mid-batch.
let inFlight = 0;
// A SERP tab pre-warmed by a 'warmup' request (warmup-before-pull). Shape:
// { tabId, engine }. runSerpBatch reuses it so jobs are only processed on a
// page that's already loaded and captcha-cleared.
let warmTab = null;

// --- Toolbar icon helpers ----------------------------------------------------

function setIcon(connected) {
  const path = connected
    ? { 16: 'icons/icon16.png', 48: 'icons/icon48.png', 128: 'icons/icon128.png' }
    : { 16: 'icons/icon16-off.png', 48: 'icons/icon48-off.png', 128: 'icons/icon128-off.png' };
  try {
    if (RT.action && RT.action.setIcon) RT.action.setIcon({ path });
    if (RT.action && RT.action.setBadgeText) {
      RT.action.setBadgeText({ text: connected ? '' : 'off' });
      if (RT.action.setBadgeBackgroundColor) {
        RT.action.setBadgeBackgroundColor({ color: connected ? '#16a34a' : '#9ca3af' });
      }
    }
  } catch (_) { /* icon may not exist on first load */ }
}

// --- WebSocket client --------------------------------------------------------

function wsConnect() {
  // Don't open a new socket if one is already open or in progress.
  if (ws && (ws.readyState === 0 || ws.readyState === 1)) return;
  try {
    ws = new WebSocket(WS_URL);
  } catch (e) {
    console.error('[WS] construct failed:', e.message);
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    console.log('[WS] connected to', WS_URL);
    wsConnected = true;
    wsReconnectDelay = RECONNECT_BASE_MS;
    setIcon(true);
    try {
      ws.send(JSON.stringify({ action: 'hello', client: 'firefox-serp-puller', version: '1.0.0' }));
    } catch (_) { /* ignore */ }
  };

  ws.onmessage = async (event) => {
    let msg;
    try { msg = JSON.parse(event.data); }
    catch (e) { console.error('[WS] non-JSON message dropped'); return; }
    // Mark the script "busy" while we run a batch so the keepalive alarm
    // knows to fire heartbeats on the WS — this prevents Firefox from
    // suspending the background page mid-batch (which kills the socket).
    inFlight += 1;
    try {
      await handleServerMessage(msg);
    } catch (err) {
      console.error('[WS] handler error:', err && err.message);
      try {
        const sock = ws;
        if (sock && sock.readyState === 1) {
          sock.send(JSON.stringify({ id: msg && msg.id, success: false, error: String(err && err.message || err) }));
        }
      } catch (_) { /* connection might be down */ }
    } finally {
      inFlight = Math.max(0, inFlight - 1);
    }
  };

  ws.onerror = (e) => {
    console.error('[WS] error:', e && e.message);
  };

  ws.onclose = () => {
    console.warn('[WS] closed');
    wsConnected = false;
    setIcon(false);
    ws = null;
    scheduleReconnect();
  };
}

function scheduleReconnect() {
  setTimeout(wsConnect, wsReconnectDelay);
  wsReconnectDelay = Math.min(wsReconnectDelay * 2, RECONNECT_MAX_MS);
}

setIcon(false);
wsConnect();

// Diagnostic: log on startup whether host permissions are actually granted.
try {
  RT.permissions.contains({ origins: ['<all_urls>'] }).then((has) => {
    console.log(`[startup] host permission <all_urls> granted = ${has}`);
    if (!has) {
      console.warn('[startup] HOST PERMISSION MISSING — open the popup and click "Grant access to all sites"');
    }
  });
} catch (_) { /* permissions API may be unavailable */ }

// Keep the background event page alive in Firefox MV3 by registering a
// periodic alarm. Without this, Firefox suspends the script after ~30s
// idle and the WebSocket dies. We also explicitly send a WS heartbeat
// when a batch is in-flight so the socket activity itself keeps the
// script awake.
try {
  RT.alarms.create('keepalive', { periodInMinutes: 0.25 });
  RT.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name !== 'keepalive') return;
    if (!ws || ws.readyState === 3) {
      console.log('[keepalive] reconnecting WS');
      wsConnect();
      return;
    }
    // Only send a heartbeat while a batch is actually in-flight — when
    // idle, MV3 may suspend us and that's fine; we'll reconnect on the
    // next batch. This stops the post-batch ws-flap storm.
    if (ws.readyState === 1 && inFlight > 0) {
      try {
        ws.send(JSON.stringify({ action: 'heartbeat', inFlight, ts: Date.now() }));
      } catch (_) { /* will retry next tick */ }
    }
  });
} catch (e) {
  console.warn('[keepalive] alarms unavailable:', e && e.message);
}

// Extra heartbeat while a batch is running. setInterval inside an MV3
// background can be paused, but in Firefox MV3 it survives as long as
// the script is awake — and the batch's own awaits keep it awake.
setInterval(() => {
  if (inFlight > 0 && ws && ws.readyState === 1) {
    try {
      ws.send(JSON.stringify({ action: 'heartbeat', inFlight, ts: Date.now() }));
    } catch (_) { /* swallow */ }
  }
}, 10_000);

// --- Top-level message dispatch ---------------------------------------------

async function handleServerMessage(msg) {
  if (!msg || typeof msg !== 'object') return;
  if (msg.action === 'runSerpBatch') {
    const result = await runSerpBatch(msg.jobs || []);
    await sendReply({ id: msg.id, success: true, results: result });
    return;
  }
  if (msg.action === 'warmup') {
    // server.py asks us to warm a SERP tab (open hello page + solve captcha)
    // BEFORE it pulls a batch. We keep the tab in `warmTab`; the next
    // runSerpBatch reuses it. Reply {ready:bool} so server.py knows whether
    // it's safe to pull.
    const res = await handleWarmup(msg.engine, msg.qtype);
    await sendReply({ id: msg.id, success: true, ...res });
    return;
  }
  if (msg.action === 'ping') {
    await sendReply({ id: msg.id, success: true, pong: Date.now() });
    return;
  }
  if (msg.action === 'heartbeat-ack') return;
  console.warn('[WS] unknown action:', msg.action);
}

// Send a reply through whatever ws is currently open. If the socket
// flapped during a long batch, wait briefly for the reconnect, then
// send. This is what makes results actually reach server.py after
// MV3 background suspend/resume cycles.
async function sendReply(payload) {
  for (let i = 0; i < 30; i++) { // up to ~15s
    if (ws && ws.readyState === 1) {
      try {
        ws.send(JSON.stringify(payload));
        return true;
      } catch (e) {
        console.warn('[WS] send failed, will retry:', e && e.message);
      }
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  console.error('[WS] sendReply gave up — no open socket');
  return false;
}

// --- Batch execution ---------------------------------------------------------

async function runSerpBatch(jobs) {
  const groups = new Map(); // key=engine/type -> [job]
  for (const j of jobs) {
    const k = `${j.engine}/${j.query_type}`;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(j);
  }
  const out = [];
  // Run groups SEQUENTIALLY so the solver only deals with one captcha at a time.
  for (const [key, groupJobs] of groups.entries()) {
    const [engine, qtype] = key.split('/');
    console.log(`[batch] ${key}: ${groupJobs.length} jobs`);
    try {
      const groupResults = await runSerpGroup(engine, qtype, groupJobs);
      out.push(...groupResults);
    } catch (err) {
      console.error(`[batch] group ${key} failed:`, err && err.message);
      for (const j of groupJobs) {
        out.push(failureResult(j, err && err.message || String(err)));
      }
    }
  }
  return out;
}

function failureResult(job, errorMessage) {
  // The management API validates `error` as a string (null is OK, boolean is
  // NOT — sending `true` causes HTTP 422). Coerce defensively.
  let errStr = null;
  if (errorMessage !== null && errorMessage !== undefined) {
    errStr = typeof errorMessage === 'string' ? errorMessage : String(errorMessage);
  }
  return {
    execution_id: job.execution_id,
    domain_name: job.domain_name,
    engine: job.engine,
    query_type: job.query_type,
    queue_name: job.queue_name,
    success: false,
    is_indexed: null,
    indexed_count: 0,
    total_results: 0,
    matched_hosts: [],
    error: errStr,
    raw: job.raw || {},
  };
}

// --- SERP group runner -------------------------------------------------------

const WORKFLOW_BY_KEY = {
  'google/index': 'workflows/serp-batch-google-web.json',
  'bing/index':   'workflows/serp-batch-bing-web.json',
  'google/news':  'workflows/serp-batch-google-news.json',
  'bing/news':    'workflows/serp-batch-bing-news.json',
};

// Cache of loaded workflow JSONs.
const _workflowCache = new Map();
async function loadWorkflow(path) {
  if (_workflowCache.has(path)) return _workflowCache.get(path);
  const url = RT.runtime.getURL(path);
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`workflow load failed ${path}: HTTP ${resp.status}`);
  const wf = await resp.json();
  _workflowCache.set(path, wf);
  return wf;
}

// Build the comma-separated query string. For "index" we use site:<domain>
// (matches what serp_match_helper checks against), for "news" we just use
// the bare domain so news engines surface stories that mention it.
function buildQueriesForJobs(qtype, jobs) {
  return jobs
    .map((j) => (qtype === 'index' ? `site:${j.domain_name}` : j.domain_name))
    .filter(Boolean)
    .join(',');
}

// Decide is_indexed from one query's result block. Each engine workflow
// returns slightly different shapes (organic_results / news_results); we
// flatten both and inspect link/domain fields.
function _apex(host) {
  if (!host) return '';
  host = String(host).toLowerCase().replace(/^www\./, '');
  const parts = host.split('.');
  if (parts.length <= 2) return host;
  return parts.slice(-2).join('.');
}
function decideMatch(domain, queryResult) {
  if (!queryResult || queryResult.error || queryResult.success === false) {
    // The per-query result uses a boolean `error: true` flag plus a `message`.
    // The management API requires the result `error` field to be a STRING, so
    // resolve to the message (or a sensible default) — never the boolean.
    const msg = queryResult && (
      queryResult.message ||
      (typeof queryResult.error === 'string' ? queryResult.error : null)
    );
    return { error: msg || 'no meaningful content', matched: [], indexed_count: 0, total_results: 0 };
  }
  const target = _apex(domain);
  const candidates = [];
  const pushFromList = (arr) => {
    if (!Array.isArray(arr)) return;
    for (const r of arr) {
      if (!r) continue;
      if (r.domain) candidates.push(r.domain);
      if (r.link)   candidates.push((() => { try { return new URL(r.link).hostname; } catch { return ''; } })());
      if (r.url)    candidates.push((() => { try { return new URL(r.url).hostname; } catch { return ''; } })());
    }
  };
  pushFromList(queryResult.organic_results);
  pushFromList(queryResult.news_results);
  pushFromList(queryResult.local_results);
  pushFromList(queryResult.top_stories);
  // indexed_count = the SERP's reported total ("About X,XXX results") parsed
  // out by the workflow as `serp_result_count`. Falls back to -1 if Google
  // didn't show a count on this query.
  let indexed_count = -1;
  const raw = queryResult.serp_result_count;
  if (raw !== undefined && raw !== null && raw !== '') {
    const n = parseInt(String(raw).replace(/[^0-9-]/g, ''), 10);
    if (!Number.isNaN(n)) indexed_count = n;
  }
  // total_results = total candidate links scraped from the SERP page.
  const total_results = candidates.length;
  // matched = which of those candidates' apex equals the target apex.
  const matched = [];
  const seen = new Set();
  for (const h of candidates) {
    const a = _apex(h);
    if (!a) continue;
    if (a === target && !seen.has(a)) { matched.push(h); seen.add(a); }
  }
  return { matched, indexed_count, total_results };
}

// Open the engine's "warmup" SERP (q=hello), recover from lazy-parked
// about:blank tabs, and solve any captcha (Buster handles audio reCAPTCHA).
// Returns a tabId sitting on a clean, cookie-trusted SERP. We use a
// low-suspicion query so the engine is less likely to challenge us; if it
// DOES, solving it here once makes the resulting cookies (NID/CONSENT/SID…)
// carry the "trusted" token through every subsequent in-page fetch().
async function prepareWarmTab(engine) {
  const warmupUrl = engine === 'google'
    ? 'https://www.google.com/search?q=hello'
    : 'https://www.bing.com/search?q=hello';
  console.log(`[warmup] opening ${warmupUrl}`);
  // active:true → foreground (visible in VNC) and never lazy-parked.
  const tab = await RT.tabs.create({ url: warmupUrl, active: true, discarded: false });
  const tabId = tab.id;

  await waitForTabComplete(tabId, SERP_TAB_TIMEOUT_MS);
  // Firefox sometimes parks tabs on about:blank even though we asked for a URL.
  let cur;
  try { cur = await RT.tabs.get(tabId); } catch (_) { cur = null; }
  const onPrivileged = !cur || !cur.url
    || cur.url.startsWith('about:')
    || cur.url.startsWith('moz-extension:')
    || cur.url === 'chrome://newtab/';
  if (onPrivileged) {
    console.warn(`[warmup] tab ${tabId} parked at ${cur && cur.url} — forcing navigation`);
    await RT.tabs.update(tabId, { url: warmupUrl, active: true });
    await waitForTabComplete(tabId, SERP_TAB_TIMEOUT_MS);
    const start = Date.now();
    while (Date.now() - start < 10000) {
      const t = await RT.tabs.get(tabId);
      if (t && t.url && !t.url.startsWith('about:') && !t.url.startsWith('moz-extension:')) {
        console.log(`[warmup] tab ${tabId} now on ${t.url}`);
        break;
      }
      await new Promise((r) => setTimeout(r, 250));
    }
  }
  // SOLVE CAPTCHA if present. After this, cookies are trusted.
  console.log(`[warmup] checking page for captcha...`);
  await ensureCaptchaSolved(tabId);
  await new Promise((r) => setTimeout(r, 1500));
  return tabId;
}

// Handle a 'warmup' request from server.py: warm a tab BEFORE the batch is
// pulled. Stores the ready tab in `warmTab` and reports whether the page is
// clean (not stuck on a /sorry/ captcha wall).
async function handleWarmup(engine, qtype) {
  engine = engine || 'google';
  // Drop any stale warm tab first.
  if (warmTab) {
    try { await RT.tabs.remove(warmTab.tabId); } catch (_) { /* gone */ }
    warmTab = null;
  }
  try {
    const tabId = await prepareWarmTab(engine);
    let url = '';
    try { const t = await RT.tabs.get(tabId); url = (t && t.url) || ''; } catch (_) {}
    if (url.includes('/sorry/')) {
      // Still captcha-walled — don't let server.py pull jobs we can't serve.
      console.warn(`[warmup] still blocked at ${url}`);
      try { await RT.tabs.remove(tabId); } catch (_) {}
      return { ready: false, reason: 'captcha not cleared', url };
    }
    warmTab = { tabId, engine };
    console.log(`[warmup] ready tab ${tabId} (url=${url})`);
    return { ready: true, url };
  } catch (e) {
    console.error('[warmup] failed:', e && e.message);
    return { ready: false, reason: (e && e.message) || String(e) };
  }
}

async function runSerpGroup(engine, qtype, jobs) {
  const key = `${engine}/${qtype}`;
  const workflowPath = WORKFLOW_BY_KEY[key];
  if (!workflowPath) throw new Error(`Unsupported engine/type: ${key}`);

  const workflow = await loadWorkflow(workflowPath);
  const queries = buildQueriesForJobs(qtype, jobs);
  const overrides = {
    queries,
    // Sensible defaults for the workflow vars; can be customised later.
    hl: 'en', gl: 'us', cc: 'US',
    sort_by: '', time_period: '', device: 'desktop', location: '',
    qft: '',
    client: 'safari',
  };

  // Reuse a tab pre-warmed by a 'warmup' request (warmup-before-pull). If
  // none is available (e.g. the MV3 background was suspended between the
  // warmup and the batch), warm one inline so we never run on a cold page.
  let tabId;
  if (warmTab && warmTab.engine === engine) {
    tabId = warmTab.tabId;
    console.log(`[batch] reusing pre-warmed tab ${tabId} for ${key}`);
    warmTab = null; // consumed; server.py re-warms before the next batch
  } else {
    console.log(`[batch] no warm tab for ${engine}; warming inline`);
    tabId = await prepareWarmTab(engine);
  }

  try {
    // Process the batch with up to MAX_BATCH_RETRIES attempts. If an attempt
    // comes back with ZERO usable results (every query blocked by captcha /
    // 429 / "no content"), we re-warm the tab — which solves any captcha via
    // Buster — and retry the whole 100-domain batch. We stop as soon as at
    // least one query returns a real SERP (indexed OR not-indexed both count
    // as "page working"), or after 7 attempts.
    let results = jobs.map((job) => failureResult(job, 'not processed'));
    for (let attempt = 1; attempt <= MAX_BATCH_RETRIES; attempt++) {
      // Make sure the page is clean before each attempt (solve captcha if any).
      await ensureCaptchaSolved(tabId);

      // Hand the tab off to the workflow runner. It executes all the actions
      // (wait_for / delay / if_exists / evaluate ...) and stores the JSON
      // string returned by the evaluate script under variables.batch_results.
      const finalVars = await self.runWorkflowOnTab(tabId, workflow, overrides);
      const parsed = _parseBatchResults(finalVars.batch_results);
      results = mapJobsToResults(jobs, qtype, parsed);

      const okCount = results.filter((r) => r.success).length;
      console.log(`[batch] ${key} attempt ${attempt}/${MAX_BATCH_RETRIES}: ${okCount}/${jobs.length} usable`);
      if (okCount > 0) break; // page is working — done

      if (attempt < MAX_BATCH_RETRIES) {
        console.warn(`[batch] ${key} all ${jobs.length} blocked — re-warming tab ${tabId} (solve captcha) and retrying`);
        await rewarmTab(tabId, engine);
      } else {
        console.error(`[batch] ${key} still blocked after ${MAX_BATCH_RETRIES} attempts — returning failures`);
      }
    }
    return results;
  } finally {
    try { await RT.tabs.remove(tabId); } catch (_) { /* gone */ }
  }
}

// Map each job to a result object using the parsed SERP batch results.
function mapJobsToResults(jobs, qtype, parsed) {
  return jobs.map((job) => {
    const queryStr = qtype === 'index' ? `site:${job.domain_name}` : job.domain_name;
    const item = parsed.byQuery[queryStr] || parsed.byIndex[jobs.indexOf(job)];
    if (!item) return failureResult(job, 'no result for query');
    const decision = decideMatch(job.domain_name, item);
    if (decision.error) return failureResult(job, decision.error);
    return {
      execution_id: job.execution_id,
      domain_name: job.domain_name,
      engine: job.engine,
      query_type: job.query_type,
      queue_name: job.queue_name,
      success: true,
      is_indexed: decision.matched.length > 0,
      indexed_count: decision.indexed_count,
      total_results: decision.total_results,
      matched_hosts: decision.matched.slice(0, 5),
      error: null,
      raw: job.raw || {},
    };
  });
}

// Re-warm an existing tab between retry attempts: reload the hello SERP and
// solve any captcha (Buster). Used when a whole batch came back blocked.
async function rewarmTab(tabId, engine) {
  const warmupUrl = engine === 'google'
    ? 'https://www.google.com/search?q=hello'
    : 'https://www.bing.com/search?q=hello';
  try {
    await RT.tabs.update(tabId, { url: warmupUrl, active: true });
    await waitForTabComplete(tabId, SERP_TAB_TIMEOUT_MS);
  } catch (e) {
    console.warn('[batch] rewarm navigation failed:', e && e.message);
  }
  await ensureCaptchaSolved(tabId);
  await new Promise((r) => setTimeout(r, 1500));
}

function _parseBatchResults(raw) {
  // Workflows wrap the result as JSON.stringify(...). google-news wraps
  // {results: [...]}, others return a bare array.
  const out = { byQuery: {}, byIndex: {} };
  let parsed = raw;
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); }
    catch (_) { parsed = null; }
  }
  let arr = null;
  if (Array.isArray(parsed)) arr = parsed;
  else if (parsed && Array.isArray(parsed.results)) arr = parsed.results;

  if (!arr) return out;
  arr.forEach((item, i) => {
    if (item && item.query) out.byQuery[item.query] = item;
    out.byIndex[i] = item;
  });
  return out;
}

// --- Tab helpers -------------------------------------------------------------

function waitForTabComplete(tabId, timeoutMs) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = async () => {
      try {
        const t = await RT.tabs.get(tabId);
        if (t && t.status === 'complete') return resolve(t);
      } catch (e) {
        return reject(new Error('tab disappeared'));
      }
      if (Date.now() - start > timeoutMs) return reject(new Error('tab load timeout'));
      setTimeout(check, 500);
    };
    check();
  });
}

async function ensureCaptchaSolved(tabId) {
  // Wait until the page is NOT showing a captcha challenge. If a Google
  // reCAPTCHA appears, we drive it through the audio-challenge solver
  // (server.py /transcribe → faster-whisper). Cloudflare Turnstile is
  // still handled by turnstile_injected.js / turnstile_bridge.js.
  const start = Date.now();
  let lastStatus = '';
  let attemptedAudioSolve = false;
  while (Date.now() - start < TURNSTILE_WAIT_MS) {
    const status = await execInTab(tabId, () => {
      try {
        // Cloudflare Turnstile token already filled in?
        const ts = document.querySelector('input[name="cf-turnstile-response"]');
        if (ts && ts.value) return 'solved';

        const onSorryUrl = location.href.includes('/sorry/');
        const sorryForm = document.querySelector('form[action*="sorry"]')
          || document.querySelector('#captcha-form');
        const recaptchaIframe = document.querySelector('iframe[src*="recaptcha"]');
        const cfIframe = document.querySelector('iframe[src*="challenges.cloudflare.com"]');

        const hasResults = !!(
          document.querySelector('#search') ||
          document.querySelector('#rso') ||
          document.querySelector('#b_results') ||
          document.querySelector('article') ||
          document.querySelector('.b_news')
        );

        if (onSorryUrl || sorryForm) return 'sorry';
        if (recaptchaIframe && !hasResults) return 'recaptcha';
        if (cfIframe && !hasResults) return 'pending';
        if (hasResults) return 'absent';
        return 'loading';
      } catch (e) { return 'pending'; }
    });

    if (status !== lastStatus) {
      console.log(`[captcha] tab ${tabId} status=${status}`);
      lastStatus = status;
    }

    if (status === 'absent' || status === 'solved') return true;

    // If reCAPTCHA / sorry page detected, hand off to the Buster Firefox
    // extension. Buster auto-detects the audio challenge — we just need to
    // make sure the reCAPTCHA challenge iframe is visible. Then we wait.
    if ((status === 'sorry' || status === 'recaptcha') && !attemptedAudioSolve) {
      attemptedAudioSolve = true;
      console.log(`[captcha] reCAPTCHA detected on tab ${tabId} — letting Buster solve it`);
      try {
        await triggerBusterSolve(tabId);
        // Buster's audio solve usually completes in 20-40s.
        await new Promise((r) => setTimeout(r, 6000));
      } catch (e) {
        console.error('[captcha] Buster trigger threw:', e && e.message);
      }
    }

    await new Promise((r) => setTimeout(r, 750));
  }
  console.warn('[captcha] solve grace expired, proceeding anyway');
  return false;
}

function execInTab(tabId, func, args = []) {
  return new Promise((resolve, reject) => {
    try {
      RT.scripting.executeScript({ target: { tabId }, func, args })
        .then((res) => {
          if (Array.isArray(res) && res.length) resolve(res[0].result);
          else resolve(undefined);
        })
        .catch(async (err) => {
          // Enrich error with the tab's current URL so we can see when Firefox
          // is blocking us on about:blank etc.
          let url = '?';
          try { const t = await RT.tabs.get(tabId); url = t && t.url; } catch (_) {}
          reject(new Error(`${err && err.message || err} (tab ${tabId} url=${url})`));
        });
    } catch (e) {
      reject(e);
    }
  });
}

// --- reCAPTCHA Buster bridge -------------------------------------------------
// We do NOT bundle a Whisper-based audio solver. The Buster Firefox
// extension (installed alongside this addon in the same profile) handles
// the audio challenge end-to-end. Our job is only to make sure the
// challenge iframe is open + audio mode selected; Buster takes it from
// there. See README → "Captcha handling".

async function findRecaptchaFrames(tabId) {
  try {
    const allFrames = await RT.webNavigation.getAllFrames({ tabId });
    let checkboxFrameId = null;
    let challengeFrameId = null;
    for (const frame of allFrames || []) {
      const u = frame && frame.url;
      if (!u) continue;
      if (u.includes('/recaptcha/api2/anchor') || u.includes('/recaptcha/enterprise/anchor')) {
        checkboxFrameId = frame.frameId;
      }
      if (u.includes('/recaptcha/api2/bframe') || u.includes('/recaptcha/enterprise/bframe')) {
        challengeFrameId = frame.frameId;
      }
    }
    return { checkboxFrameId, challengeFrameId };
  } catch (e) {
    return { checkboxFrameId: null, challengeFrameId: null };
  }
}

async function executeInFrame(tabId, frameId, func, args = []) {
  try {
    const results = await RT.scripting.executeScript({
      target: { tabId, frameIds: [frameId] },
      world: 'MAIN',
      func,
      args,
    });
    return results && results[0] ? results[0].result : null;
  } catch (e) {
    console.warn(`[recaptcha] executeInFrame frameId=${frameId} err=${e && e.message}`);
    return null;
  }
}

async function clickInFrame(tabId, frameId, selector) {
  return executeInFrame(tabId, frameId, (sel) => {
    const el = document.querySelector(sel);
    if (!el) return false;
    try { el.scrollIntoView({ block: 'center' }); } catch (_) {}
    // Focus first — Buster's help-button-holder is tabindex-focusable and
    // its handler wakes up on focus + click + Enter, not just click.
    try { el.focus({ preventScroll: true }); } catch (_) {}
    // Synthetic mouse events.
    ['mousedown', 'mouseup', 'click'].forEach((t) => {
      el.dispatchEvent(new MouseEvent(t, { bubbles: true, cancelable: true, view: window, button: 0 }));
    });
    // Native .click() (works on HTMLElement, fires whatever the browser
    // would on a real click — ignored by some custom event listeners
    // but cheap to try).
    try { if (typeof el.click === 'function') el.click(); } catch (_) {}
    // Enter keypress — many tabindex-focusable widgets activate on Enter.
    ['keydown', 'keypress', 'keyup'].forEach((t) => {
      el.dispatchEvent(new KeyboardEvent(t, { bubbles: true, cancelable: true, key: 'Enter', code: 'Enter', keyCode: 13, which: 13 }));
    });
    return true;
  }, [selector]);
}

// === Inlined Buster-style audio reCAPTCHA solver ============================
// We do NOT depend on the Buster extension. Instead we replicate Buster's
// flow inside our own background:
//   1. Click reCAPTCHA anchor checkbox.
//   2. Wait for challenge bframe.
//   3. Click "I'm not a robot — try audio version" button.
//   4. Pull the MP3 audio URL from the bframe.
//   5. Download it, decode via Web Audio, encode as 16-bit PCM WAV.
//   6. POST to https://api.wit.ai/speech with one of Buster's English
//      bearer tokens (rotated to spread out 429s).
//   7. Fill #audio-response, click recaptcha-verify-button.
//
// Tokens are taken from Buster's encrypted secrets.txt (decrypted offline
// once and embedded). They are rotated on each use; if all 8 are 429ed
// simultaneously, the solver returns false and the caller falls back to
// timing out the captcha.
const WIT_TOKENS = [
  'XQJHYVR4KONXQ67GUMNXC4W2EPDCHN7R',
  'YXXJQA67PWKV47Q77PZCUEQ5Z2MPIXOU',
  'PHSN24KB3AUAH6O6XAJ5UAPBN65MUCCY',
  'P6X4YWXBC75WH2LCDEKG2RN7GZ2R4Y4G',
  'MVXHXO3F7M7NTYZYY3RPSDMMUAJMFF7H',
  'D5BISQ3FKXK247ETWSA4OH2D3FJXBLLN',
  'WH3OX3Y3PGGNRZOFKW6WETNFJII5QB2D',
  'LEXH24F65MX2TPKVLLLQXDRSWOAEGZMT',
];
let _witIdx = Math.floor(Math.random() * WIT_TOKENS.length);
function nextWitToken() {
  const t = WIT_TOKENS[_witIdx % WIT_TOKENS.length];
  _witIdx = (_witIdx + 1) % WIT_TOKENS.length;
  return t;
}

// Encode an AudioBuffer as a 16-bit PCM WAV ArrayBuffer.
// (Simple/inline — replaces Buster's audiobuffer-to-wav webpack module.)
function audioBufferToWav(buffer) {
  const numCh = buffer.numberOfChannels;
  const sr = buffer.sampleRate;
  const bps = 16;
  const samples = (numCh === 2)
    ? interleave(buffer.getChannelData(0), buffer.getChannelData(1))
    : buffer.getChannelData(0);
  const bytesPerSample = bps / 8;
  const blockAlign = numCh * bytesPerSample;
  const byteRate = sr * blockAlign;
  const dataSize = samples.length * bytesPerSample;
  const buf = new ArrayBuffer(44 + dataSize);
  const v = new DataView(buf);
  let p = 0;
  function writeStr(s) { for (let i = 0; i < s.length; i++) v.setUint8(p++, s.charCodeAt(i)); }
  writeStr('RIFF'); v.setUint32(p, 36 + dataSize, true); p += 4;
  writeStr('WAVE'); writeStr('fmt '); v.setUint32(p, 16, true); p += 4;
  v.setUint16(p, 1, true); p += 2;            // PCM
  v.setUint16(p, numCh, true); p += 2;
  v.setUint32(p, sr, true); p += 4;
  v.setUint32(p, byteRate, true); p += 4;
  v.setUint16(p, blockAlign, true); p += 2;
  v.setUint16(p, bps, true); p += 2;
  writeStr('data'); v.setUint32(p, dataSize, true); p += 4;
  for (let i = 0; i < samples.length; i++, p += 2) {
    let s = Math.max(-1, Math.min(1, samples[i]));
    v.setInt16(p, s < 0 ? s * 0x8000 : s * 0x7fff, true);
  }
  return buf;
}
function interleave(L, R) {
  const out = new Float32Array(L.length + R.length);
  let i = 0, j = 0;
  while (i < out.length) { out[i++] = L[j]; out[i++] = R[j]; j++; }
  return out;
}

async function transcribeAudioWithWit(audioUrl) {
  // Download MP3
  const mp3 = await fetch(audioUrl, { credentials: 'omit' }).then(r => {
    if (!r.ok) throw new Error(`audio download HTTP ${r.status}`);
    return r.arrayBuffer();
  });
  // Decode (Web Audio API works in Firefox MV3 background page)
  const ctx = new (self.AudioContext || self.webkitAudioContext)();
  const decoded = await ctx.decodeAudioData(mp3);
  ctx.close && ctx.close();
  // Encode as WAV
  const wav = audioBufferToWav(decoded);
  // POST to wit.ai (try each token until one returns 200; skip 429s)
  let lastErr = '';
  for (let i = 0; i < WIT_TOKENS.length; i++) {
    const token = nextWitToken();
    const resp = await fetch('https://api.wit.ai/speech?v=20240304', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'audio/wav' },
      body: new Blob([wav], { type: 'audio/wav' }),
      credentials: 'omit',
    });
    if (resp.status === 200) {
      // wit.ai streams a sequence of JSON objects separated by newlines —
      // newer responses use chunked event-stream. The final object has the
      // full transcript in .text
      const text = await resp.text();
      // Split by lines/objects and grab the LAST one with a "text" field
      let transcript = '';
      // wit.ai now returns a JSON array of intermediate + final results.
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
          for (const obj of parsed) if (obj && obj.text) transcript = obj.text;
        } else if (parsed && parsed.text) {
          transcript = parsed.text;
        }
      } catch (_) {
        // Fallback: ndjson — extract last text:"..." occurrence
        const matches = [...text.matchAll(/"text"\s*:\s*"([^"]*)"/g)];
        if (matches.length) transcript = matches[matches.length - 1][1];
      }
      transcript = (transcript || '').trim();
      if (transcript) return transcript;
      lastErr = 'wit.ai returned empty transcript';
      continue;
    }
    if (resp.status === 429) { lastErr = 'wit.ai 429 (rate-limited)'; continue; }
    lastErr = `wit.ai HTTP ${resp.status}`;
    break;
  }
  throw new Error(lastErr || 'wit.ai unknown error');
}

// Top-level reCAPTCHA solver — replaces the previous "click Buster icon" path.
async function solveRecaptcha(tabId, totalWaitMs = 90000) {
  let frames = await findRecaptchaFrames(tabId);
  // 1. Click anchor checkbox if not already checked.
  if (frames.checkboxFrameId) {
    const checked = await executeInFrame(tabId, frames.checkboxFrameId, () => {
      const a = document.querySelector('span#recaptcha-anchor');
      return a && a.getAttribute('aria-checked') === 'true';
    });
    if (!checked) {
      console.log('[recaptcha] clicking anchor checkbox');
      await clickInFrame(tabId, frames.checkboxFrameId, 'span#recaptcha-anchor');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  // 2. Wait for challenge bframe.
  for (let i = 0; i < 20; i++) {
    frames = await findRecaptchaFrames(tabId);
    if (frames.challengeFrameId) break;
    await new Promise((r) => setTimeout(r, 500));
  }
  if (!frames.challengeFrameId) {
    console.log('[recaptcha] no challenge frame — solved by checkbox alone');
    return true;
  }
  // 3. Switch to audio challenge.
  console.log('[recaptcha] switching to audio challenge');
  await clickInFrame(tabId, frames.challengeFrameId, 'button#recaptcha-audio-button');
  // Wait for the audio source to appear.
  let audioUrl = null;
  for (let i = 0; i < 30 && !audioUrl; i++) {
    audioUrl = await executeInFrame(tabId, frames.challengeFrameId, () => {
      const a = document.querySelector('audio#audio-source');
      return a && (a.getAttribute('src') || a.src) || null;
    });
    if (!audioUrl) await new Promise((r) => setTimeout(r, 500));
  }
  if (!audioUrl) {
    console.warn('[recaptcha] no audio URL appeared — possible "automated queries" block');
    return false;
  }
  console.log('[recaptcha] audio URL:', audioUrl.slice(0, 80) + '…');
  // 4. Transcribe via wit.ai.
  let transcript = '';
  try {
    transcript = await transcribeAudioWithWit(audioUrl);
    console.log('[recaptcha] wit.ai transcript:', transcript);
  } catch (e) {
    console.error('[recaptcha] transcription failed:', e && e.message);
    return false;
  }
  // 5. Fill #audio-response and click verify.
  await executeInFrame(tabId, frames.challengeFrameId, (text) => {
    const inp = document.querySelector('input#audio-response');
    if (!inp) return false;
    inp.focus();
    inp.value = text;
    inp.dispatchEvent(new Event('input', { bubbles: true }));
    return true;
  }, [transcript]);
  await new Promise((r) => setTimeout(r, 500));
  console.log('[recaptcha] clicking verify');
  await clickInFrame(tabId, frames.challengeFrameId, 'button#recaptcha-verify-button');
  // 6. Poll for solved state.
  const start = Date.now();
  while (Date.now() - start < totalWaitMs) {
    frames = await findRecaptchaFrames(tabId);
    if (!frames.checkboxFrameId) return true;
    const checked = await executeInFrame(tabId, frames.checkboxFrameId, () => {
      const a = document.querySelector('span#recaptcha-anchor');
      return a && a.getAttribute('aria-checked') === 'true';
    });
    if (checked) {
      console.log('[recaptcha] solved');
      return true;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  console.warn('[recaptcha] not confirmed solved before timeout');
  return false;
}

// Backwards-compatible alias — the old caller uses triggerBusterSolve().
async function triggerBusterSolve(tabId, totalWaitMs = 90000) {
  return solveRecaptcha(tabId, totalWaitMs);
}

// --- Turnstile click bridge handler -----------------------------------------
// turnstile_bridge.js posts CHECKBOX_POSITION_RATIO from the iframe; in
// Firefox we can't drive a CDP click like Chrome does, so we instead
// dispatch synthetic events at the reported ratio. Cloudflare's audio
// fallback requires the user to actually press it — Buster automation
// is invoked next as a backup.

RT.runtime.onMessage.addListener((msg, sender) => {
  if (!msg || msg.action !== 'detectAndClickTurnstile') return;
  if (!sender.tab) return;
  const tabId = sender.tab.id;
  const { xRatio, yRatio } = msg.payload || {};
  // Synthetic click at the inner-frame ratio. (The reported ratios are
  // relative to the iframe's window; clicking on the OUTER tab at that
  // ratio works for the standard managed challenge widget.)
  execInTab(tabId, (xR, yR) => {
    const x = Math.round(window.innerWidth * xR);
    const y = Math.round(window.innerHeight * yR);
    const el = document.elementFromPoint(x, y);
    if (!el) return false;
    ['mousedown', 'mouseup', 'click'].forEach((type) => {
      el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y }));
    });
    return true;
  }, [xRatio, yRatio]).catch((e) => console.warn('[turnstile] synth click failed:', e && e.message));
});

// --- Popup status -----------------------------------------------------------

let lastBatchSummary = null;
RT.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg && msg.action === 'getStatus') {
    sendResponse({ connected: wsConnected, lastBatch: lastBatchSummary });
    return true;
  }
  if (msg && msg.action === 'reconnectWs') {
    console.log('[popup] forcing WS reconnect');
    try { if (ws) ws.close(); } catch (_) {}
    ws = null;
    wsConnected = false;
    setIcon(false);
    setTimeout(wsConnect, 200);
    sendResponse({ ok: true });
    return true;
  }
});

// Wrap runSerpBatch to record summary for the popup.
const _origRunSerpBatch = runSerpBatch;
runSerpBatch = async function (jobs) {
  const t0 = Date.now();
  try {
    const results = await _origRunSerpBatch(jobs);
    const passed = results.filter((r) => r.success).length;
    lastBatchSummary = `${results.length} jobs in ${(Date.now() - t0)}ms (pass=${passed})`;
    return results;
  } catch (e) {
    lastBatchSummary = `error: ${e && e.message}`;
    throw e;
  }
};
