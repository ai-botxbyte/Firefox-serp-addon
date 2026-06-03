// SERP runner content script — currently a placeholder. Heavy lifting
// happens via `chrome.scripting.executeScript` from background.js so the
// in-page fetch loop has access to the page's session cookies. This file
// is included so future per-engine scrapers (e.g. JS-rendered SERP layouts)
// can register selectors here.
console.log('[serp_runner] loaded on', location.href);
