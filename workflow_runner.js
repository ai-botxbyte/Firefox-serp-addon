// Workflow runner — executes a small subset of the BotXByte workflow JSON
// schema needed by the SERP batch workflows shipped under /workflows/.
//
// Action types supported: navigate, wait_for, delay, if_exists (with nested
// actions / else_actions), click, evaluate (with set_variable),
// recaptcha_solve. Anything else throws so we notice.
//
// All execution happens in the page context via chrome.scripting.executeScript
// from background.js — no separate content script required.

const RT_WF = (typeof browser !== 'undefined' && browser.runtime) ? browser : chrome;

function _resolveString(s, vars) {
  if (typeof s !== 'string') return s;
  return s.replace(/\$\{(\w+)\}/g, (_, k) => (vars && vars[k] !== undefined && vars[k] !== null) ? String(vars[k]) : '');
}

function _execInTabRunner(tabId, func, args = []) {
  return RT_WF.scripting.executeScript({ target: { tabId }, func, args, world: 'MAIN' })
    .then((res) => Array.isArray(res) && res.length ? res[0].result : undefined);
}

async function _waitForTab(tabId, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const t = await RT_WF.tabs.get(tabId);
      if (t && t.status === 'complete') return t;
    } catch (e) { throw new Error('tab disappeared'); }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error('tab load timeout');
}

async function _waitFor(tabId, selectors, timeoutMs) {
  const result = await RT_WF.scripting.executeScript({
    target: { tabId },
    func: async (sels, t) => {
      const start = Date.now();
      while (Date.now() - start < t) {
        for (const s of sels) {
          try {
            if (s.startsWith('text:')) {
              const text = s.slice(5).toLowerCase();
              const el = [...document.querySelectorAll('*')].find(e => e.textContent && e.textContent.toLowerCase().includes(text));
              if (el) return true;
            } else if (s.startsWith('xpath:')) {
              const r = document.evaluate(s.slice(6), document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
              if (r.singleNodeValue) return true;
            } else if (document.querySelector(s)) {
              return true;
            }
          } catch (_) { /* bad selector */ }
        }
        await new Promise(r => setTimeout(r, 200));
      }
      return false;
    },
    args: [selectors, timeoutMs],
  });
  return Array.isArray(result) && result.length ? !!result[0].result : false;
}

async function _ifExists(tabId, selectors, timeoutMs) {
  // Same as _waitFor but with 0-ms or short timeout semantics handled by caller.
  return _waitFor(tabId, selectors, timeoutMs);
}

async function _click(tabId, selectors) {
  const r = await RT_WF.scripting.executeScript({
    target: { tabId },
    func: (sels) => {
      for (const s of sels) {
        try {
          let el = null;
          if (s.startsWith('xpath:')) {
            const x = document.evaluate(s.slice(6), document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
            el = x.singleNodeValue;
          } else if (s.startsWith('text:')) {
            const text = s.slice(5).toLowerCase();
            el = [...document.querySelectorAll('*')].find(e => e.textContent && e.textContent.toLowerCase().includes(text));
          } else {
            el = document.querySelector(s);
          }
          if (el) {
            el.scrollIntoView({ block: 'center' });
            ['mousedown', 'mouseup', 'click'].forEach(t =>
              el.dispatchEvent(new MouseEvent(t, { bubbles: true, cancelable: true, view: window }))
            );
            return true;
          }
        } catch (_) { /* try next */ }
      }
      return false;
    },
    args: [selectors],
  });
  return Array.isArray(r) && r.length ? !!r[0].result : false;
}

async function _evaluate(tabId, scriptText, vars) {
  // Substitute ${var} into script text first (workflow templating).
  const resolved = _resolveString(scriptText, vars);
  // Pass the raw script body (NOT pre-wrapped). The page-side wrapper builds
  // an AsyncFunction from it and invokes it once.
  const out = await RT_WF.scripting.executeScript({
    target: { tabId },
    world: 'MAIN',
    func: async (body) => {
      try {
        // Build an async function whose body is the workflow's evaluate code.
        // The script may use `await` and `return` at the top level — that's
        // why we wrap it in an AsyncFunction rather than eval'ing directly.
        const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
        const fn = new AsyncFunction(body);
        const v = await fn();
        return { ok: true, value: v };
      } catch (e) {
        return { ok: false, error: (e && e.message) ? e.message : String(e) };
      }
    },
    args: [resolved],
  });
  const res = Array.isArray(out) && out.length ? out[0].result : null;
  if (!res || !res.ok) {
    throw new Error('evaluate failed: ' + (res && res.error ? res.error : 'unknown'));
  }
  return res.value;
}

async function _recaptchaSolve(_tabId) {
  // Buster-style audio solve is not bundled; if the in-page Buster addon is
  // installed alongside, it will pick up automatically. Here we just wait
  // a few seconds to give any installed solver a chance.
  await new Promise((r) => setTimeout(r, 6000));
  return true;
}

// Exported entrypoint used by background.js
async function runWorkflowOnTab(tabId, workflow, vars) {
  const variables = { ...(workflow.variables || {}), ...(vars || {}) };
  const actions = workflow.actions || [];

  for (let i = 0; i < actions.length; i++) {
    const a = actions[i];
    const type = a.type;
    if (type === 'navigate') {
      const url = _resolveString(a.url, variables);
      await RT_WF.tabs.update(tabId, { url });
      await _waitForTab(tabId, a.timeout || 30000);
    } else if (type === 'wait_for') {
      const ok = await _waitFor(tabId, a.selectors || [], a.timeout || 5000);
      if (!ok) throw new Error(`wait_for timeout: ${(a.selectors || []).join(',')}`);
    } else if (type === 'delay') {
      await new Promise((r) => setTimeout(r, a.timeout || 0));
    } else if (type === 'if_exists') {
      const exists = await _ifExists(tabId, a.selectors || [], a.timeout || 0);
      const branch = exists ? (a.actions || a.then || []) : (a.else_actions || a.else || []);
      if (branch && branch.length) {
        for (const sub of branch) {
          await runSingleAction(tabId, sub, variables);
        }
      }
    } else if (type === 'click') {
      await _click(tabId, a.selectors || []);
    } else if (type === 'evaluate') {
      const out = await _evaluate(tabId, a.script || '', variables);
      if (a.set_variable) variables[a.set_variable] = out;
    } else if (type === 'recaptcha_solve') {
      await _recaptchaSolve(tabId);
    } else {
      throw new Error('unsupported workflow action: ' + type);
    }
  }
  return variables;
}

async function runSingleAction(tabId, a, variables) {
  if (a.type === 'click') return _click(tabId, a.selectors || []);
  if (a.type === 'delay') return new Promise((r) => setTimeout(r, a.timeout || 0));
  if (a.type === 'wait_for') {
    const ok = await _waitFor(tabId, a.selectors || [], a.timeout || 5000);
    if (!ok) throw new Error('wait_for timeout');
    return;
  }
  if (a.type === 'recaptcha_solve') return _recaptchaSolve(tabId);
  if (a.type === 'evaluate') {
    const out = await _evaluate(tabId, a.script || '', variables);
    if (a.set_variable) variables[a.set_variable] = out;
    return;
  }
  if (a.type === 'navigate') {
    const url = _resolveString(a.url, variables);
    await RT_WF.tabs.update(tabId, { url });
    await _waitForTab(tabId, a.timeout || 30000);
    return;
  }
  throw new Error('unsupported nested action: ' + a.type);
}

self.runWorkflowOnTab = runWorkflowOnTab;
