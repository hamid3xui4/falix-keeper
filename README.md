# falix-keeper — auto-restart for Falix server 3521489

Every 5 minutes this checks the server and, if it is down, starts it through the
official Falix console WebSocket. It also writes `keeper-state.json` counting how
many times it has revived the server and why.

## Setup (5 steps)
1. Create a **PUBLIC** GitHub repository, e.g. `falix-keeper`. (Public = unlimited Actions minutes.)
2. Upload `keeper.py` and `.github/workflows/falix-keeper.yml` **preserving the folder structure**.
3. Repo → Settings → Secrets and variables → Actions → **New repository secret**
   - Name: `FALIX_API_KEY`  Value: your Falix Client API key
   - (optional) `TG_TOKEN`, `TG_CHAT` for Telegram alerts
4. Repo → Actions tab → enable workflows → open **falix-keeper** → **Run workflow** once to test.
5. Watch the run log: `state=running — healthy, nothing to do` means it works.

## What it does when the server is down
- sends `set state start` on the console socket (the only route that works on the free plan —
  `POST /power` asks for captcha and `falixnodes.net/startserver` uses Cloudflare Turnstile);
- if Falix answers `no free memory`, it retries on the next tick;
- if Falix answers `policy: ... not allowed on this plan`, it **still restarts** (as instructed),
  increments `policy_stops`, waits 120 s, and sends an alert.

## Honest limit
Restarting after a *policy* stop is not a fix. Falix documents that clear-cut violations can lead
to automatic **account** suspension, and account suspensions have **no appeal process**. The counter
in `keeper-state.json` exists so you can see the escalation rate and move off in time.
