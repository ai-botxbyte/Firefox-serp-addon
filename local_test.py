#!/usr/bin/env python3
"""Local end-to-end test: 100 google/index jobs → Firefox addon → results.

Runs three things in one process:
  1. A mock domain-metrics-management-service exposing
     POST /api/v1/serp-queue/pull/ and /api/v1/serp-queue/result/.
  2. The Firefox addon's `server.py` poller pointed at the mock.
  3. (You) load the addon into Firefox manually before running, OR pass
     --launch-firefox to have web-ext launch it for you.

When all 100 jobs come back (or 5 min timeout), prints a summary.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
from aiohttp import web

HERE = Path(__file__).parent
TEST_DOMAINS = [
    "google.com", "facebook.com", "youtube.com", "twitter.com", "instagram.com",
    "wikipedia.org", "amazon.com", "apple.com", "microsoft.com", "netflix.com",
    "reddit.com", "linkedin.com", "github.com", "stackoverflow.com", "bing.com",
    "yahoo.com", "duckduckgo.com", "openai.com", "anthropic.com", "huggingface.co",
    "nytimes.com", "bbc.com", "cnn.com", "theverge.com", "techcrunch.com",
    "medium.com", "dev.to", "hashnode.com", "vercel.com", "netlify.com",
    "cloudflare.com", "digitalocean.com", "heroku.com", "aws.amazon.com", "azure.microsoft.com",
    "ycombinator.com", "producthunt.com", "indiehackers.com", "kickstarter.com", "patreon.com",
    "stripe.com", "paypal.com", "shopify.com", "wordpress.com", "wix.com",
    "squarespace.com", "godaddy.com", "namecheap.com", "wpengine.com", "kinsta.com",
    "spotify.com", "soundcloud.com", "twitch.tv", "discord.com", "slack.com",
    "zoom.us", "notion.so", "asana.com", "trello.com", "monday.com",
    "atlassian.com", "jira.com", "gitlab.com", "bitbucket.org", "circleci.com",
    "jenkins.io", "docker.com", "kubernetes.io", "redhat.com", "ubuntu.com",
    "debian.org", "fedoraproject.org", "archlinux.org", "kernel.org", "mozilla.org",
    "rust-lang.org", "python.org", "nodejs.org", "go.dev", "java.com",
    "djangoproject.com", "flask.palletsprojects.com", "fastapi.tiangolo.com", "expressjs.com", "nestjs.com",
    "nextjs.org", "svelte.dev", "vuejs.org", "angular.io", "reactjs.org",
    "tailwindcss.com", "bootstrap.com", "webpack.js.org", "vite.dev", "esbuild.github.io",
    "npmjs.com", "yarnpkg.com", "pnpm.io", "rubygems.org", "pypi.org",
    "crates.io", "packagist.org", "cocoapods.org", "swiftpackageindex.com", "maven.apache.org",
]
assert len(TEST_DOMAINS) >= 100, "need 100 domains"
TEST_DOMAINS = TEST_DOMAINS[:100]

BATCH_QUEUE: List[Dict[str, Any]] = []
RECEIVED: List[Dict[str, Any]] = []
DONE_EVENT = asyncio.Event()
TARGET_TOTAL = 100


def seed_jobs():
    BATCH_QUEUE.clear()
    for d in TEST_DOMAINS:
        BATCH_QUEUE.append({
            "execution_id": str(uuid.uuid4()),
            "domain_name": d,
            "engine": "google",
            "query_type": "index",
            "queue_name": "google.index-check-queue",
            "raw": {"domain_name": d, "test": True},
        })


async def pull_handler(req):
    body = await req.json() if req.body_exists else {}
    n = int((body or {}).get("message_count", 100))
    take = BATCH_QUEUE[:n]
    del BATCH_QUEUE[:n]
    print(f"[mock] /pull/ -> {len(take)} job(s) ({len(BATCH_QUEUE)} remaining in queue)")
    return web.json_response({
        "success": True,
        "data": {
            "jobs": [
                {
                    "execution_id": j["execution_id"],
                    "domain_name": j["domain_name"],
                    "engine": j["engine"],
                    "query_type": j["query_type"],
                    "queue_name": j["queue_name"],
                    "raw": j["raw"],
                }
                for j in take
            ],
            "pulled": len(take),
        },
        "message": f"Pulled {len(take)} job(s)",
    })


async def result_handler(req):
    body = await req.json()
    items = body.get("results") or []
    RECEIVED.extend(items)
    pass_n = sum(1 for r in items if r.get("success"))
    fail_n = len(items) - pass_n
    indexed_n = sum(1 for r in items if r.get("is_indexed") is True)
    not_indexed_n = sum(1 for r in items if r.get("is_indexed") is False)
    print(f"[mock] /result/ +{len(items)} (success={pass_n} fail={fail_n} indexed={indexed_n} not_indexed={not_indexed_n}) total received={len(RECEIVED)}")
    if len(RECEIVED) >= TARGET_TOTAL:
        DONE_EVENT.set()
    return web.json_response({
        "success": True,
        "data": {"published": len(items), "failed": 0, "errors": []},
        "message": "ok",
    })


async def run_mock_mgmt(port: int):
    app = web.Application()
    app.router.add_post("/api/v1/serp-queue/pull/", pull_handler)
    app.router.add_post("/api/v1/serp-queue/result/", result_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    print(f"[mock] mgmt-service stub listening on http://127.0.0.1:{port}")
    return runner


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mgmt-port", type=int, default=8210)
    ap.add_argument("--server-py", type=str, default=str(HERE / "server.py"))
    ap.add_argument("--launch-firefox", action="store_true",
                    help="Use web-ext to launch a temporary Firefox profile with the addon loaded")
    ap.add_argument("--timeout", type=int, default=300)
    args = ap.parse_args()

    seed_jobs()
    mgmt_runner = await run_mock_mgmt(args.mgmt_port)

    # Launch addon's server.py
    env = dict(os.environ)
    env["MGMT_BASE_URL"] = f"http://127.0.0.1:{args.mgmt_port}"
    env["POLL_INTERVAL_SECS"] = "3"
    env["BATCH_SIZE"] = "100"
    print(f"[harness] launching server.py against {env['MGMT_BASE_URL']}")
    server_proc = subprocess.Popen(
        [sys.executable, args.server_py],
        env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        bufsize=1, text=True,
    )

    async def stream_output():
        loop = asyncio.get_event_loop()
        while True:
            line = await loop.run_in_executor(None, server_proc.stdout.readline)
            if not line:
                break
            print(f"[server.py] {line.rstrip()}")

    out_task = asyncio.create_task(stream_output())

    # Optionally launch Firefox via web-ext
    fx_proc = None
    if args.launch_firefox:
        print("[harness] launching Firefox via web-ext run …")
        fx_proc = subprocess.Popen(
            ["npx", "--yes", "web-ext", "run", "--source-dir", str(HERE),
             "--no-reload", "--browser-console"],
            cwd=str(HERE),
        )
    else:
        print("\n" + "="*70)
        print("MANUAL STEP: open Firefox, go to about:debugging#/runtime/this-firefox,")
        print(f"click 'Load Temporary Add-on…', and pick:")
        print(f"  {HERE}/manifest.json")
        print("Then watch the addon icon turn green. Test will continue automatically.")
        print("="*70 + "\n")

    # Wait for completion or timeout
    try:
        await asyncio.wait_for(DONE_EVENT.wait(), timeout=args.timeout)
        print("\n[harness] ✅ all results received")
    except asyncio.TimeoutError:
        print(f"\n[harness] ⚠️  timeout after {args.timeout}s — got {len(RECEIVED)}/{TARGET_TOTAL}")

    # Summary
    print("\n" + "="*70)
    print(f"RESULTS: {len(RECEIVED)} / {TARGET_TOTAL}")
    pass_n = [r for r in RECEIVED if r.get("success")]
    fail_n = [r for r in RECEIVED if not r.get("success")]
    indexed = [r for r in pass_n if r.get("is_indexed")]
    not_indexed = [r for r in pass_n if r.get("is_indexed") is False]
    print(f"  success: {len(pass_n)}")
    print(f"  failure: {len(fail_n)}")
    print(f"  indexed=True : {len(indexed)}")
    print(f"  indexed=False: {len(not_indexed)}")
    if fail_n:
        print("\nFirst 5 failures:")
        for r in fail_n[:5]:
            print(f"  - {r.get('domain_name')}: {r.get('error')}")
    if indexed[:5]:
        print("\nFirst 5 indexed:")
        for r in indexed[:5]:
            print(f"  - {r.get('domain_name')} matched={r.get('matched_hosts')}")
    print("="*70)

    # Clean up
    server_proc.terminate()
    if fx_proc:
        try: fx_proc.terminate()
        except Exception: pass
    out_task.cancel()
    await mgmt_runner.cleanup()
    return 0 if len(RECEIVED) == TARGET_TOTAL else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
