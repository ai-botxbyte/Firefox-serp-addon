// Proxy configuration for the SERP addon.
//
// Firefox's proxy is set DIRECTLY to PROXY_HOST:PROXY_PORT (via the portable
// autoconfig.cfg), but Firefox prefs can't carry a username/password — so
// background.js registers a webRequest.onAuthRequired handler that supplies
// these credentials automatically (no login popup).
//
// The CI workflow regenerates this file from plaintext env vars at build time;
// the committed values below are the defaults used for local/portable runs.
self.PROXY_AUTH = {
  host: "p.webshare.io",
  port: 80,
  username: "tjijutki-rotate",
  password: "4vg93ifc50gnx",
};
