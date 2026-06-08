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

// 2captcha API key — used ONLY as a fallback when the reCAPTCHA audio
// challenge has no play button (audio blocked). The CI workflow regenerates
// this from a plaintext env var at build time.
self.TWOCAPTCHA = {
  apiKey: "cc659aa1b0cff87b80e3969a281f8d3f",
};
