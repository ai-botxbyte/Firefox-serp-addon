// Turnstile Auto-Click: Bridge script (ISOLATED world)
// Forwards checkbox position from MAIN world (turnstile_injected.js) to the
// background script. Firefox MV3 exposes both `browser.*` and `chrome.*`
// namespaces; we prefer browser if present.
(function () {
  const RT = (typeof browser !== 'undefined' && browser.runtime) ? browser.runtime
            : (typeof chrome !== 'undefined' && chrome.runtime) ? chrome.runtime
            : null;
  if (!RT) return;
  if (window.top === window.self) return;
  if (!window.location.href.includes('challenges.cloudflare.com')) return;

  window.addEventListener('message', (event) => {
    if (event.source !== window) return;
    if (!event.data || event.data.type !== 'CHECKBOX_POSITION_RATIO') return;
    const { xRatio, yRatio } = event.data.payload || {};
    try {
      const p = RT.sendMessage({
        action: 'detectAndClickTurnstile',
        payload: { xRatio, yRatio },
      });
      if (p && typeof p.catch === 'function') {
        p.catch((err) => console.error('[Turnstile Bridge] sendMessage failed:', err && err.message));
      }
    } catch (e) {
      console.error('[Turnstile Bridge] sendMessage threw:', e && e.message);
    }
  }, false);
})();
