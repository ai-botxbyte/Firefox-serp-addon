// Proxy configuration for the SERP addon.
//
// Firefox's proxy is set DIRECTLY to PROXY_HOST:PROXY_PORT (via the portable
// autoconfig.cfg), but Firefox prefs can't carry a username/password — so
// background.js registers a webRequest.onAuthRequired handler that supplies
// these credentials automatically (no login popup).
//
// The CI workflow regenerates this file from plaintext env vars at build time;
// the committed values below are the defaults used for local/portable runs.
// Proxy is used in IP-AUTHORIZATION mode (no username/password) — the
// runner/host IP is authorized on webshare, so Firefox connects to
// p.webshare.io:9999 with no credentials. PROXY_AUTH is left blank; the
// onAuthRequired handler stays unregistered when there's no username.
self.PROXY_AUTH = {
  host: "p.webshare.io",
  port: 9999,
  username: "",
  password: "",
};

// 2captcha API key — used ONLY as a fallback when the reCAPTCHA audio
// challenge has no play button (audio blocked). The CI workflow regenerates
// this from a plaintext env var at build time.
self.TWOCAPTCHA = {
  apiKey: "cc659aa1b0cff87b80e3969a281f8d3f",
};
