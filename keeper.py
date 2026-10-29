#!/usr/bin/env python3
"""
falix-keeper — always-on auto-start keeper for Falix app servers.

WHY THIS EXISTS
  On the Falix free plan the node evicts/stops servers when it runs out of
  memory ("the node has no free memory for another server right now") and the
  plan has no auto-restart.  Only an EXTERNAL process can bring the server back.
  This keeper must therefore run on an always-on host that is NOT the Falix box.

BEHAVIOUR
  * polls /servers/{id}/status every POLL_SECONDS (default 45)
  * state == offline  -> start via console WebSocket  {"event":"set state","args":["start"]}
                        (REST POST /power needs a captcha on the free plan, so WS is used)
  * node out of memory -> backs off and retries; never gives up
  * after start, verifies the public port with a TLS handshake
  * optional Telegram alert via TG_TOKEN / TG_CHAT

USAGE
  export FALIX_API_KEY=...            # account API key
  export FALIX_SERVER_ID=3521489
  export FALIX_PROBE_HOST=157.90.205.61
  export FALIX_PROBE_PORT=23132
  export FALIX_PROBE_SNI=mrphon3.falix.org
  python3 keeper.py --loop            # or run once via cron every minute
Dependencies: websockets  (pip install websockets)
"""
import argparse, json, os, random, socket, ssl, sys, time, urllib.error, urllib.request

API = "https://client.falixnodes.net/api/v2"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
KEY = os.environ.get("FALIX_API_KEY", "")
SID = os.environ.get("FALIX_SERVER_ID", "3521489")
POLL = int(os.environ.get("POLL_SECONDS", "45"))
HOST = os.environ.get("FALIX_PROBE_HOST", "")
PORT = int(os.environ.get("FALIX_PROBE_PORT", "0") or 0)
SNI = os.environ.get("FALIX_PROBE_SNI", "")
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")
ALIVE = ("running", "starting", "stopping", "installing", "restoring", "transferring")


def log(*a):
    print(time.strftime("[%Y-%m-%dT%H:%M:%SZ]", time.gmtime()), *a, flush=True)


def alert(text):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        body = json.dumps({"chat_id": TG_CHAT, "text": text}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                                     data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as e:
        log("alert failed:", type(e).__name__)


def api(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Authorization": f"Bearer {KEY}",
                                          "Content-Type": "application/json",
                                          "Accept": "application/json, text/plain, */*",
                                          "User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return {"error": json.loads(e.read().decode()).get("error", {"code": f"http_{e.code}"})}
        except Exception:
            return {"error": {"code": f"http_{e.code}"}}
    except Exception as e:
        return {"error": {"code": type(e).__name__}}


def state():
    r = api(f"/servers/{SID}/status")
    if "error" in r:
        return None, r["error"].get("code")
    return r.get("data", {}).get("state"), None


def probe():
    if not (HOST and PORT):
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.set_alpn_protocols(["h2"])
    except Exception:
        pass
    try:
        with socket.create_connection((HOST, PORT), timeout=8) as s:
            with ctx.wrap_socket(s, server_hostname=SNI or None) as ss:
                return ss.version()
    except Exception as e:
        return f"FAIL:{type(e).__name__}"


def start_ws():
    """Start the server through the console WebSocket (free-plan friendly)."""
    sess = api(f"/servers/{SID}/console/token", method="POST", body={})
    if "error" in sess:
        return False, f"token:{sess['error'].get('code')}"
    s = sess["data"]
    try:
        import asyncio
        import websockets

        async def _go():
            seen = []
            async with websockets.connect(s["socket"],
                                          origin="https://client.falixnodes.net",
                                          user_agent_header=UA,
                                          open_timeout=25,
                                          max_size=4 * 1024 * 1024) as ws:
                await ws.send(json.dumps({"event": "auth", "args": [s["token"]]}))
                deadline = time.time() + 5
                while time.time() < deadline:
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    except asyncio.TimeoutError:
                        break
                    if m.get("event") == "auth success":
                        break
                await ws.send(json.dumps({"event": "set state", "args": ["start"]}))
                end = time.time() + 45
                while time.time() < end:
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=4))
                    except asyncio.TimeoutError:
                        continue
                    ev = m.get("event")
                    if ev == "console output":
                        line = (m.get("args") or [""])[0]
                        seen.append(line)
                        low = line.lower()
                        if "no free memory" in low:
                            return False, "node_out_of_memory"
                        if "policy:" in low and "not allowed on this plan" in low:
                            return False, "policy_stop"
                        if "not allowed on this plan" in low:
                            return False, "policy_stop"
                    elif ev == "status":
                        st = (m.get("args") or [None])[0]
                        seen.append(f"status={st}")
                        if st in ("running", "starting"):
                            return True, st
                return False, "; ".join(seen[-3:]) or "no status"

        return asyncio.run(_go())
    except ModuleNotFoundError:
        return False, "missing dependency: pip install websockets"
    except Exception as e:
        return False, f"{type(e).__name__}"


STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "keeper-state.json")
POLICY_BACKOFF = int(os.environ.get("POLICY_BACKOFF_SECONDS", "120"))


def bump(reason):
    """Persist a restart counter so the owner can see how often it is being revived."""
    d = {"restarts_total": 0, "by_reason": {}, "last_reason": None, "last_at": None}
    try:
        d.update(json.load(open(STATE_FILE)))
    except Exception:
        pass
    d["restarts_total"] += 1
    d["by_reason"][reason] = d["by_reason"].get(reason, 0) + 1
    d["last_reason"] = reason
    d["last_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        json.dump(d, open(STATE_FILE, "w"), indent=1)
    except Exception:
        pass
    return d


def start_api():
    """Try POST /power; returns (ok, code, message, action_url). Never raises."""
    r = api(f"/servers/{SID}/power", method="POST", body={"signal": "start"})
    if isinstance(r, dict) and "error" in r:
        e = r["error"]
        return False, e.get("code"), e.get("message"), e.get("action_url")
    return True, None, None, None


def tick(verbose=True):
    st, err = state()
    if err:
        if verbose:
            log("status check failed:", err, "- retrying later")
        return
    if st in ALIVE:
        if verbose:
            log(f"state={st} — healthy, nothing to do")
        return
    log(f"state={st} — DETECTED DOWN, starting")
    ok, detail = start_ws()
    log(f"start -> {ok} ({detail})")
    if ok:
        d = bump("down")
        log(f"restarts_total={d['restarts_total']} by_reason={d['by_reason']}")
        time.sleep(8)
        p = probe()
        log("public probe:", p)
        alert(f"Falix Node2 was down ({st}) — keeper started it. probe={p} restarts={d['restarts_total']}")
    elif not ok and (detail or "").startswith("status=") or detail == "no status":
        # WS start was accepted by the socket but nothing happened: ask the API why
        ok2, code, msg, url = start_api()
        log(f"api fallback -> ok={ok2} code={code}")
        if not ok2:
            d = bump(str(code))
            if code == "verification_required":
                log(f"VERIFICATION REQUIRED — open within 5 min: {url}")
                alert(f"Falix needs a HUMAN verification (captcha) before it will start. "
                      f"Open within 5 minutes: {url} | total_restarts={d['restarts_total']}")
            else:
                alert(f"Falix refused start: {code} — {msg}")
            time.sleep(30)
    elif detail == "policy_stop":
        d = bump("policy_stop")
        log(f"POLICY STOP acknowledged — restarting anyway as instructed "
            f"(restarts_total={d['restarts_total']} policy={d['by_reason']['policy_stop']})")
        alert(f"Falix stopped the server by POLICY. Restarting as instructed. "
              f"policy_stops={d['by_reason']['policy_stop']} total={d['restarts_total']}. "
              f"This can escalate to a permanent account suspension.")
        time.sleep(POLICY_BACKOFF)
    elif detail == "node_out_of_memory":
        log("node has no free memory — will retry on next tick")
        alert("Falix Node2 down; node reports no free memory, retrying.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true", help="run forever")
    ap.add_argument("--max-runtime", type=int, default=0,
                    help="exit after N seconds (for CI jobs), 0 = unlimited")
    a = ap.parse_args()
    if not KEY:
        sys.exit("FALIX_API_KEY is required")
    if not a.loop:
        tick()
        return
    t0 = time.time()
    while True:
        try:
            tick()
        except Exception as e:
            log("tick error:", type(e).__name__, e)
        if a.max_runtime and time.time() - t0 > a.max_runtime:
            log("max runtime reached, exiting cleanly")
            return
        time.sleep(POLL + random.randint(0, 10))


if __name__ == "__main__":
    main()
