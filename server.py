#!/usr/bin/env python3
"""Local pull-mode SERP server for the BotXByte Firefox addon.

Loop:
  1. Wait for the Firefox addon to connect on ws://localhost:8765.
  2. Every POLL_INTERVAL_SECS, GET up to 100 SERP jobs from
     <MGMT_BASE_URL>/api/v1/serp-queue/pull/.
  3. Forward the batch to the addon via WebSocket
     ({"action":"runSerpBatch","id":"<uuid>","jobs":[...]}).
  4. Wait for the addon's reply, POST results back to
     <MGMT_BASE_URL>/api/v1/serp-queue/result/.
  5. Loop.

If the addon disconnects mid-batch, the in-flight jobs are dropped (the
results never reach the management service); the corresponding workflow
will simply time out the same way it would on an APISerpent failure.

Usage:
    python3 server.py --mode local       # file-based (domains.txt -> results.json)
    python3 server.py --mode prod        # poll prod management API
    python3 server.py --mode prod --engine bing --query-type index
    python3 server.py                    # defaults to local

Env (overridable, but --mode handles all common defaults):
    MGMT_BASE_URL              prod default: https://b-domain.articleinnovator.com/domain-metrics-management-service
    MGMT_AUTH_TOKEN            optional bearer token (not needed — endpoints are public)
    POLL_INTERVAL_SECS         default 5
    BATCH_SIZE                 default 100
    WS_PORT                    default 8765
    HTTP_PORT                  default 8766 (lightweight status endpoint)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import web
import websockets

# Prefix every log line with an ISO timestamp so progress is traceable.
import builtins as _builtins
from datetime import datetime as _dt


def print(*args, **kwargs):  # noqa: A001 - intentional module-level shadow
    ts = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
    _builtins.print(f"[{ts}]", *args, **kwargs)

# Audio reCAPTCHA solving is delegated to the Buster Firefox extension —
# the user explicitly does NOT want a local Whisper model bundled here.
# Buster runs IN Firefox alongside this addon and solves audio challenges
# itself. We keep /transcribe out of the API surface entirely.


# ---------------------------------------------------------------------------
# CLI: --mode local | prod
# ---------------------------------------------------------------------------
PROD_MGMT_BASE_URL = "https://b-domain.articleinnovator.com/domain-metrics-management-service"
LOCAL_MGMT_BASE_URL = "http://localhost:8210"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Firefox SERP extension — local pull-mode server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python3 server.py --mode local\n"
            "  python3 server.py --mode prod\n"
            "  python3 server.py --mode prod --engine bing --query-type index\n"
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["local", "prod"],
        default="local",
        help="local = read domains.txt, write results.json (default). "
             "prod  = poll https://b-domain.articleinnovator.com management API.",
    )
    parser.add_argument("--engine", choices=["google", "bing"], default=None,
                        help="(local mode only) override LOCAL_ENGINE, default google")
    parser.add_argument("--query-type", choices=["index", "news"], default=None,
                        help="(local mode only) override LOCAL_QUERY_TYPE, default index")
    return parser.parse_args()


_ARGS = _parse_args()
MODE = _ARGS.mode  # "local" | "prod"


# ---------------------------------------------------------------------------
# Configuration — derived from --mode, then overridable via env
# ---------------------------------------------------------------------------
_default_base = PROD_MGMT_BASE_URL if MODE == "prod" else LOCAL_MGMT_BASE_URL
MGMT_BASE_URL = os.getenv("MGMT_BASE_URL", _default_base).rstrip("/")
MGMT_AUTH_TOKEN = os.getenv("MGMT_AUTH_TOKEN", "")
MGMT_WORKSPACE_ID = os.getenv("MGMT_WORKSPACE_ID", "")
SERP_PULL_PATH = os.getenv("SERP_PULL_PATH", "/api/v1/serp-queue/pull/")
SERP_RESULT_PATH = os.getenv("SERP_RESULT_PATH", "/api/v1/serp-queue/result/")
POLL_INTERVAL_SECS = float(os.getenv("POLL_INTERVAL_SECS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
WS_PORT = int(os.getenv("WS_PORT", "8765"))
HTTP_PORT = int(os.getenv("HTTP_PORT", "8766"))
BATCH_TIMEOUT_SECS = float(os.getenv("BATCH_TIMEOUT_SECS", "180"))
PUBLISH_MAX_RETRIES = int(os.getenv("PUBLISH_MAX_RETRIES", "3"))  # transient publish retries

# --- LOCAL MODE (file-based, no management service) -------------------------
# In local mode, server.py ignores MGMT_BASE_URL pull/result entirely. Instead
# it reads domains from DOMAINS_FILE (one per line), forwards them as SERP
# jobs to the Firefox addon, and writes results to RESULTS_FILE.
# In prod mode, this flag is False and the API poll loop runs.
LOCAL_MODE = (MODE == "local")
DOMAINS_FILE = os.getenv("DOMAINS_FILE", os.path.join(os.path.dirname(__file__), "domains.txt"))
RESULTS_FILE = os.getenv("RESULTS_FILE", os.path.join(os.path.dirname(__file__), "results.json"))
LOCAL_ENGINE = _ARGS.engine or os.getenv("LOCAL_ENGINE", "google")          # google|bing
LOCAL_QUERY_TYPE = _ARGS.query_type or os.getenv("LOCAL_QUERY_TYPE", "index")  # index|news

ws_conn: Optional[websockets.WebSocketServerProtocol] = None
pending: Dict[str, asyncio.Future] = {}
status_state = {
    "ws_connected": False,
    "last_pull_count": 0,
    "last_publish_count": 0,
    "last_publish_failed": 0,
    "loops": 0,
    "errors": 0,
    "last_error": None,
}


def _auth_headers() -> Dict[str, str]:
    h = {"Content-Type": "application/json"}
    if MGMT_AUTH_TOKEN:
        h["Authorization"] = f"Bearer {MGMT_AUTH_TOKEN}"
    if MGMT_WORKSPACE_ID:
        h["workspace-id"] = MGMT_WORKSPACE_ID
    return h


async def ws_handler(ws):
    """Single addon connection at a time. When the MV3 background script is
    suspended/resumed by Firefox, it opens a NEW socket — the old one is
    dead but TCP hasn't told us yet. Always trust the latest connection;
    drop the previous one (if any) so the live addon can deliver results."""
    global ws_conn
    prev = ws_conn
    if prev is not None and prev is not ws:
        print(f"♻️  new WS from {ws.remote_address}; closing stale previous socket")
        try:
            await prev.close(code=4001, reason="superseded")
        except Exception:
            pass
    ws_conn = ws
    status_state["ws_connected"] = True
    print(f"✅ Firefox addon connected from {ws.remote_address}")
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                print(f"⚠️ non-JSON ws message: {raw[:200]}")
                continue
            req_id = msg.get("id")
            action = msg.get("action")
            if req_id and req_id in pending and not pending[req_id].done():
                pending[req_id].set_result(msg)
            elif action == "hello":
                print(f"👋 hello from addon: {msg}")
            elif action in ("heartbeat", "heartbeat-ack"):
                # Silent — keeps the MV3 background alive between batches.
                pass
            else:
                print(f"⚠️ ws message with unknown id: {msg}")
    except websockets.exceptions.ConnectionClosed:
        # Normal: either the addon closed or we superseded this socket.
        pass
    finally:
        if ws_conn is ws:
            ws_conn = None
            status_state["ws_connected"] = False
            print("❌ Firefox addon disconnected")


async def pull_jobs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    url = f"{MGMT_BASE_URL}{SERP_PULL_PATH}"
    payload = {"queue_name": "serp.browser-pool-queue", "message_count": BATCH_SIZE}
    try:
        async with session.post(url, json=payload, headers=_auth_headers(), timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status >= 400:
                body = await resp.text()
                print(f"❌ pull HTTP {resp.status}: {body[:500]}")
                return []
            data = await resp.json()
    except Exception as e:
        print(f"❌ pull error: {e}")
        status_state["errors"] += 1
        status_state["last_error"] = f"pull: {e}"
        return []

    body = data.get("data") or {}
    jobs = body.get("jobs") or []
    status_state["last_pull_count"] = len(jobs)
    if jobs:
        print(f"📥 pulled {len(jobs)} job(s)")
    else:
        print("📭 pull returned 0 jobs (queue empty) — nothing to process")
    return jobs


async def push_results(session: aiohttp.ClientSession, results: List[Dict[str, Any]]) -> None:
    if not results:
        return
    url = f"{MGMT_BASE_URL}{SERP_RESULT_PATH}"
    # Publish with retries. Transient failures (network / 5xx) are retried up
    # to PUBLISH_MAX_RETRIES times with backoff. A 4xx (e.g. 422 schema error)
    # is deterministic — retrying won't help — so we log details and stop.
    for attempt in range(1, PUBLISH_MAX_RETRIES + 1):
        try:
            async with session.post(url, json={"results": results}, headers=_auth_headers(), timeout=aiohttp.ClientTimeout(total=60)) as resp:
                text = await resp.text()
                try:
                    payload = json.loads(text)
                except Exception:
                    payload = {"raw": text[:500]}
                data = payload.get("data") or {}
                published = data.get("published", 0)
                failed = data.get("failed", 0)
                status_state["last_publish_count"] = published
                status_state["last_publish_failed"] = failed
                print(f"📤 published={published} failed={failed} status={resp.status} (attempt {attempt}/{PUBLISH_MAX_RETRIES})")

                if resp.status < 400:
                    return  # success

                # Dump the error so we can see the validation/upstream problem.
                detail = payload.get("detail") if isinstance(payload, dict) else None
                if isinstance(detail, list):
                    print(f"  ⚠️ HTTP {resp.status} validation errors:")
                    for err in detail[:10]:
                        loc = ".".join(str(x) for x in err.get("loc", []))
                        print(f"     - {loc}: {err.get('msg')} (type={err.get('type')})")
                    if len(detail) > 10:
                        print(f"     ... and {len(detail) - 10} more")
                else:
                    print(f"  ⚠️ HTTP {resp.status} body: {text[:800]}")
                try:
                    sample = results[0] if results else {}
                    print(f"  📦 first result payload sample (truncated): "
                          f"{json.dumps({k: sample.get(k) for k in ('execution_id','domain_name','engine','query_type','queue_name','success','is_indexed','error')}, default=str)[:400]}")
                except Exception:
                    pass

                if resp.status < 500:
                    # 4xx is deterministic — don't waste retries.
                    status_state["errors"] += 1
                    status_state["last_error"] = f"push HTTP {resp.status}"
                    return
                # 5xx: fall through to retry.
        except Exception as e:
            status_state["errors"] += 1
            status_state["last_error"] = f"push: {e}"
            print(f"❌ result push error (attempt {attempt}/{PUBLISH_MAX_RETRIES}): {e}")

        if attempt < PUBLISH_MAX_RETRIES:
            backoff = min(2 ** attempt, 15)
            print(f"  ↻ retrying publish in {backoff}s...")
            await asyncio.sleep(backoff)
    print(f"❌ publish failed after {PUBLISH_MAX_RETRIES} attempts")


async def run_batch_with_addon(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Wait briefly for the addon if it hasn't connected yet (Firefox boot,
    # MV3 background respawn, …). Avoids "deferring batch" on the first run.
    waited = 0
    while not ws_conn and waited < 30:
        await asyncio.sleep(0.5)
        waited += 1
    if not ws_conn:
        print("⚠️ addon not connected; deferring batch")
        return []
    req_id = str(uuid.uuid4())
    fut: asyncio.Future = asyncio.Future()
    pending[req_id] = fut
    payload = json.dumps({"action": "runSerpBatch", "id": req_id, "jobs": jobs})
    # Try to send. If the socket flaps between send and reply, the addon
    # background will reconnect and any new ws_handler will route the
    # reply by id — so we just need to make sure SOME ws got the request.
    sent = False
    for attempt in range(60):  # up to ~30s waiting for a live socket
        if ws_conn is not None:
            try:
                await ws_conn.send(payload)
                sent = True
                break
            except Exception as e:
                print(f"⚠️ ws send failed (attempt {attempt}): {e}")
        await asyncio.sleep(0.5)
    if not sent:
        print("❌ could not deliver batch to addon (socket never became writable)")
        pending.pop(req_id, None)
        return []
    try:
        try:
            reply = await asyncio.wait_for(fut, timeout=BATCH_TIMEOUT_SECS)
        except asyncio.TimeoutError:
            print("❌ batch timed out waiting for addon")
            return []
    finally:
        pending.pop(req_id, None)

    if not reply.get("success"):
        print(f"❌ addon reported failure: {reply.get('error')}")
        return []
    return reply.get("results") or []


async def warmup_with_addon() -> bool:
    """Ask the addon to open the hello SERP and solve any captcha BEFORE we
    pull a batch (warmup-before-pull). Returns True only when the addon
    reports the page is loaded and clean — so we never pull jobs we can't
    serve. The warm tab is kept on the addon side and reused by the batch."""
    waited = 0
    while not ws_conn and waited < 30:
        await asyncio.sleep(0.5)
        waited += 1
    if not ws_conn:
        print("⚠️ addon not connected; cannot warm up")
        return False

    req_id = str(uuid.uuid4())
    fut: asyncio.Future = asyncio.Future()
    pending[req_id] = fut
    payload = json.dumps({
        "action": "warmup", "id": req_id,
        "engine": LOCAL_ENGINE, "qtype": LOCAL_QUERY_TYPE,
    })
    sent = False
    for attempt in range(60):  # up to ~30s waiting for a live socket
        if ws_conn is not None:
            try:
                await ws_conn.send(payload)
                sent = True
                break
            except Exception as e:
                print(f"⚠️ ws warmup send failed (attempt {attempt}): {e}")
        await asyncio.sleep(0.5)
    if not sent:
        print("❌ could not deliver warmup to addon")
        pending.pop(req_id, None)
        return False

    try:
        try:
            reply = await asyncio.wait_for(fut, timeout=BATCH_TIMEOUT_SECS)
        except asyncio.TimeoutError:
            print("❌ warmup timed out waiting for addon")
            return False
    finally:
        pending.pop(req_id, None)

    ready = bool(reply.get("ready"))
    if ready:
        print(f"🔥 warmup ready (url={reply.get('url')})")
    else:
        print(f"⚠️ warmup not ready: {reply.get('reason')} (url={reply.get('url')})")
    return ready


async def poll_loop():
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            status_state["loops"] += 1
            try:
                if not ws_conn:
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue
                # WARMUP-BEFORE-PULL: open the hello SERP and clear any captcha
                # FIRST. Only pull a batch once the page is confirmed clean, so
                # we don't consume queue jobs we can't actually serve.
                warm = await warmup_with_addon()
                if not warm:
                    # Captcha unsolved / addon not ready. Back off longer so we
                    # don't hammer the hello page (which can get the IP walled).
                    await asyncio.sleep(max(POLL_INTERVAL_SECS, 15))
                    continue
                jobs = await pull_jobs(session)
                if not jobs:
                    # Queue empty. Back off longer so an idle queue doesn't make
                    # us re-open the hello page every few seconds.
                    await asyncio.sleep(max(POLL_INTERVAL_SECS, 15))
                    continue
                results = await run_batch_with_addon(jobs)
                if results:
                    await push_results(session, results)
            except Exception as e:
                status_state["errors"] += 1
                status_state["last_error"] = str(e)
                print(f"❌ poll loop error: {e}")
                await asyncio.sleep(POLL_INTERVAL_SECS)


# --- LOCAL MODE loop ---------------------------------------------------------

def _load_domains_from_file() -> List[str]:
    if not os.path.exists(DOMAINS_FILE):
        return []
    with open(DOMAINS_FILE, "r", encoding="utf-8") as f:
        domains = []
        for line in f:
            d = line.strip()
            if not d or d.startswith("#"):
                continue
            domains.append(d)
        return domains


def _build_jobs_from_domains(domains: List[str]) -> List[Dict[str, Any]]:
    queue_name = f"{LOCAL_ENGINE}.{LOCAL_QUERY_TYPE}-check-queue"
    return [
        {
            "execution_id": str(uuid.uuid4()),
            "domain_name": d,
            "engine": LOCAL_ENGINE,
            "query_type": LOCAL_QUERY_TYPE,
            "queue_name": queue_name,
            "raw": {"domain_name": d, "local_test": True},
        }
        for d in domains
    ]


def _write_results(all_results: List[Dict[str, Any]]) -> None:
    try:
        passed = sum(1 for r in all_results if r.get("success"))
        failed = len(all_results) - passed
        indexed = sum(1 for r in all_results if r.get("is_indexed") is True)
        not_indexed = sum(1 for r in all_results if r.get("is_indexed") is False)
        unknown_indexed = sum(1 for r in all_results if r.get("is_indexed") is None)
        total_indexed_hits = sum(int(r.get("indexed_count") or 0) for r in all_results)
        output = {
            "summary": {
                "total": len(all_results),
                "success": passed,
                "failure": failed,
                "indexed_count": indexed,
                "not_indexed_count": not_indexed,
                "unknown_indexed_count": unknown_indexed,
                "total_indexed_hits": total_indexed_hits,
                "engine": LOCAL_ENGINE,
                "query_type": LOCAL_QUERY_TYPE,
            },
            "results": all_results,
        }
        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"❌ failed to write {RESULTS_FILE}: {e}")


async def local_loop():
    """Read domains.txt once the addon connects, run ONE batch for the
    lifetime of this server.py process, dump results.json. Subsequent
    addon reconnects do NOT re-fire the batch — restart server.py to
    re-run."""
    batch_done = False
    batch_running = False
    while True:
        if batch_done:
            await asyncio.sleep(5)
            continue
        if batch_running:
            await asyncio.sleep(1)
            continue
        if not ws_conn:
            await asyncio.sleep(1)
            continue
        # Tiny settle delay so the addon's "hello" has been processed
        await asyncio.sleep(0.5)
        if not ws_conn:
            continue
        batch_running = True
        domains = _load_domains_from_file()
        if not domains:
            print(f"⚠️ no domains found in {DOMAINS_FILE}; add one per line and restart server.py")
            batch_done = True
            batch_running = False
            continue
        print(f"📄 loaded {len(domains)} domain(s) from {DOMAINS_FILE}")
        print(f"🚀 firing ALL {len(domains)} queries in a single batch (no chunking, all parallel in-page)")
        all_results: List[Dict[str, Any]] = []
        try:
            jobs = _build_jobs_from_domains(domains)
            results = await run_batch_with_addon(jobs)
            err_count = sum(1 for r in results if not r.get("success"))
            ok_count = len(results) - err_count
            print(f"📥 batch done: ok={ok_count} err={err_count}")
            all_results.extend(results)
            status_state["last_pull_count"] = len(jobs)
            status_state["last_publish_count"] = len(results)
            _write_results(all_results)
        except Exception as e:
            print(f"❌ batch error: {e}")
        # Final write
        _write_results(all_results)
        print(f"💾 wrote {len(all_results)} result(s) to {RESULTS_FILE}")
        # Pretty summary
        passed = sum(1 for r in all_results if r.get("success"))
        failed = len(all_results) - passed
        indexed = sum(1 for r in all_results if r.get("is_indexed") is True)
        not_indexed = sum(1 for r in all_results if r.get("is_indexed") is False)
        print("=" * 60)
        print(f"  total   : {len(all_results)}")
        print(f"  success : {passed}")
        print(f"  failure : {failed}")
        print(f"  indexed : {indexed}")
        print(f"  not idx : {not_indexed}")
        print("=" * 60)
        for r in all_results:
            mark = "✅" if r.get("success") else "❌"
            idx = r.get("is_indexed")
            idx_s = "INDEXED" if idx is True else ("NOT-INDEXED" if idx is False else "?")
            cnt = r.get("indexed_count") or 0
            err = f" err={r.get('error')}" if not r.get("success") else ""
            print(f"  {mark} {r.get('domain_name'):40s} {idx_s:11s} hits={cnt}{err}")
        print("✅ batch complete — restart server.py to re-run")
        batch_done = True
        batch_running = False


# --- HTTP status endpoint ----------------------------------------------------

async def status_handler(_req):
    return web.json_response({
        **status_state,
        "ws_port": WS_PORT,
        "mgmt_base_url": MGMT_BASE_URL,
        "poll_interval_secs": POLL_INTERVAL_SECS,
        "batch_size": BATCH_SIZE,
        "captcha_solver": "buster-extension (in-firefox)",
    })


# /transcribe endpoint removed — captcha audio solving is delegated entirely
# to the Buster Firefox extension. See README.


async def main():
    # Silence noisy WebSocket handshake tracebacks. These fire when something
    # opens a plain TCP connection to the WS port without a WebSocket
    # handshake (port-readiness probes, scanners). They're harmless — the real
    # addon connects fine — but the stack traces clutter the log.
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    print(f"🚀 mode: {MODE.upper()}")
    print(f"🔌 ws://localhost:{WS_PORT} (Firefox addon)")
    print(f"🌐 http://localhost:{HTTP_PORT}/status")
    if LOCAL_MODE:
        print(f"🧪 LOCAL MODE: reading domains from {DOMAINS_FILE}")
        print(f"   → engine={LOCAL_ENGINE} query_type={LOCAL_QUERY_TYPE}")
        print(f"   → results will be written to {RESULTS_FILE}")
    else:
        print(f"📡 PROD MODE: {MGMT_BASE_URL}{SERP_PULL_PATH}")
        print(f"               {MGMT_BASE_URL}{SERP_RESULT_PATH}")
        print(f"⏱️  poll interval: {POLL_INTERVAL_SECS}s, batch size: {BATCH_SIZE}")

    await websockets.serve(ws_handler, "localhost", WS_PORT, max_size=20 * 1024 * 1024)
    if LOCAL_MODE:
        asyncio.create_task(local_loop())
    else:
        asyncio.create_task(poll_loop())

    app = web.Application()
    app.router.add_get("/status", status_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "localhost", HTTP_PORT).start()

    await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nbye")
        sys.exit(0)
