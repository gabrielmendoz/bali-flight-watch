#!/usr/bin/env python3
"""
Bali Flight Watch — scans the cheapest one-way ARN (Stockholm) -> DPS (Bali)
fare for each departure date in a window, then WhatsApps the cheapest via Fonnte.

Data source: Apify actor memo23/google-flights-scraper (live Google Flights).
No calendar mode, so we fire one bounded async run per date and take the min.

Env vars required:
  APIFY_TOKEN     - Apify API token
  FONNTE_TOKEN    - Fonnte WhatsApp API token
  OWNER_WHATSAPP  - recipient number (e.g. 628xxxxxxxxxx)

Flags:
  --no-whatsapp   - scan + write results.json only, don't send a message
"""
import os, sys, json, time, urllib.request, urllib.error
from datetime import date, timedelta

ORIGIN      = "ARN"
DESTINATION = "DPS"
WINDOW_START = date(2026, 10, 1)
WINDOW_END   = date(2026, 10, 15)   # inclusive
ADULTS      = 1
CURRENCY    = "USD"
MAX_FLIGHTS = 25                    # pull enough that 1-stop options survive the filter
MAX_STOPS   = 1                     # only clean itineraries (<=1 connection)
ACTOR       = "makework36~flight-price-scraper"  # multi-source, ultra-cheap
RUN_MEM     = 512                   # MB per run
CONCURRENCY = 4                     # max simultaneous Apify runs
MAX_CHARGE  = "0.05"               # per-run USD cost ceiling

HERE        = os.path.dirname(os.path.abspath(__file__))
RESULTS     = os.path.join(HERE, "results.json")
STATE       = os.path.join(HERE, "state.json")   # remembers last best price for delta
# Manual "refresh now" target. Override with env TRIGGER_URL to point at a
# one-click proxy; defaults to the GitHub Actions run-workflow page.
TRIGGER_URL = os.environ.get(
    "TRIGGER_URL",
    "https://github.com/gabrielmendoz/bali-flight-watch/actions/workflows/watch.yml")

APIFY_TOKEN    = os.environ.get("APIFY_TOKEN", "")
FONNTE_TOKEN   = os.environ.get("FONNTE_TOKEN", "")
OWNER_WHATSAPP = os.environ.get("OWNER_WHATSAPP", "")


def _req(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def daterange():
    d = WINDOW_START
    while d <= WINDOW_END:
        yield d
        d += timedelta(days=1)


def start_run(dep_date):
    url = (f"https://api.apify.com/v2/acts/{ACTOR}/runs"
           f"?token={APIFY_TOKEN}&timeout=180&memory={RUN_MEM}"
           f"&maxTotalChargeUsd={MAX_CHARGE}")
    body = {"origin": ORIGIN, "destination": DESTINATION,
            "departDate": dep_date, "adults": ADULTS,
            "currency": CURRENCY, "maxFlights": MAX_FLIGHTS,
            "maxStops": str(MAX_STOPS)}
    resp = _req("POST", url, body, {"Content-Type": "application/json"})
    return resp["data"]["id"], resp["data"]["defaultDatasetId"]


def run_status(run_id):
    return _req("GET", f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}")["data"]["status"]


def dataset_items(ds_id):
    return _req("GET", f"https://api.apify.com/v2/datasets/{ds_id}/items?token={APIFY_TOKEN}&limit={MAX_FLIGHTS}")


def _hhmm(iso):
    return iso[11:16] if isinstance(iso, str) and len(iso) >= 16 else None


def _ymd(iso):
    return iso[0:10] if isinstance(iso, str) and len(iso) >= 10 else None


def cheapest_from(items):
    cands = []
    for x in items:
        if not isinstance(x, dict):
            continue
        bp = x.get("bestPrice")
        st = x.get("stops")
        if not isinstance(bp, (int, float)):
            continue
        if isinstance(st, int) and st > MAX_STOPS:   # enforce <=1 stop client-side
            continue
        cands.append(x)
    if not cands:
        return None
    best = min(cands, key=lambda x: x["bestPrice"])
    return {
        "price": best["bestPrice"],
        "currency": best.get("currency", "USD") or "USD",
        "carrier": best.get("airline", "?"),
        "source": best.get("cheapestSource"),
        "stops": best.get("stops"),
        "depart_time": _hhmm(best.get("departTime")),
        "arrive_date": _ymd(best.get("arriveTime")),
        "arrive_time": _hhmm(best.get("arriveTime")),
        "duration_str": best.get("duration"),
    }


def gflights_link(dep_date):
    return (f"https://www.google.com/travel/flights?q=Flights%20{ORIGIN}%20to%20"
            f"{DESTINATION}%20on%20{dep_date}%20oneway")


def scan():
    print(f"Scanning {ORIGIN}->{DESTINATION}, {WINDOW_START}..{WINDOW_END} "
          f"(mem {RUN_MEM}MB, {CONCURRENCY} at a time)")
    queue   = [d.isoformat() for d in daterange()]
    active  = {}          # ds -> {run_id, ds_id}
    results = {}

    def fill():
        while queue and len(active) < CONCURRENCY:
            ds = queue.pop(0)
            try:
                rid, dsid = start_run(ds)
                active[ds] = {"run_id": rid, "ds_id": dsid}
                print(f"  started {ds}: {rid}")
            except Exception as e:
                results[ds] = None
                print(f"  FAILED to start {ds}: {e}")

    fill()
    for _ in range(120):  # up to ~12 min ceiling
        if not active and not queue:
            break
        for ds in list(active.keys()):
            try:
                st = run_status(active[ds]["run_id"])
            except Exception:
                continue
            if st in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                r = active.pop(ds)
                if st == "SUCCEEDED":
                    try:
                        results[ds] = cheapest_from(dataset_items(r["ds_id"]))
                        p = results[ds]["price"] if results[ds] else None
                        print(f"  {ds}: {'$'+str(p) if p else 'no fares'}")
                    except Exception as e:
                        results[ds] = None
                        print(f"  {ds}: dataset error {e}")
                else:
                    results[ds] = None
                    print(f"  {ds}: run {st}")
        fill()
        if active or queue:
            time.sleep(5)

    rows = []
    for d in daterange():
        ds = d.isoformat()
        best = results.get(ds)
        rows.append({"date": ds, "weekday": d.strftime("%a"),
                     "link": gflights_link(ds), **(best or {"price": None})})

    priced = [r for r in rows if r.get("price") is not None]
    overall = min(priced, key=lambda r: r["price"]) if priced else None

    out = {
        "route": f"{ORIGIN} → {DESTINATION}",
        "origin_name": "Stockholm", "dest_name": "Bali",
        "window": f"{WINDOW_START.isoformat()} to {WINDOW_END.isoformat()}",
        "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "rows": rows,
        "cheapest": overall,
    }
    with open(RESULTS, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Wrote {RESULTS} ({len(priced)}/{len(rows)} dates priced)")
    return out


def render_dashboard(out):
    """Write a self-contained dashboard.html (data baked in, opens via file://)."""
    rows = out["rows"]
    priced = [r for r in rows if r.get("price") is not None]
    lo = min((r["price"] for r in priced), default=0)
    hi = max((r["price"] for r in priced), default=1)
    span = (hi - lo) or 1
    c = out.get("cheapest")

    bars = []
    for r in rows:
        p = r.get("price")
        is_best = c and r["date"] == c["date"]
        if p is None:
            pct, label, cls = 6, "—", "bar miss"
        else:
            pct = 12 + 88 * (1 - (p - lo) / span)   # cheapest = longest bar
            label = f"${p:.0f}"
            cls = "bar best" if is_best else "bar"
        d = r["date"][8:10]
        bars.append(
            f'<a class="row{" best" if is_best else ""}" href="{r["link"]}" target="_blank">'
            f'<span class="d"><b>{r["weekday"]}</b> {d} Oct</span>'
            f'<span class="track"><span class="{cls}" style="width:{pct:.1f}%"></span></span>'
            f'<span class="p">{label}</span></a>')

    if c:
        stops = "nonstop" if c.get("stops") == 0 else f'{c.get("stops")} stop'
        dur_s = c.get("duration_str") or ""
        src = f' · via {c.get("source")}' if c.get("source") else ""
        hero = (
            f'<div class="price">${c["price"]:.0f}</div>'
            f'<div class="when">{c["weekday"]} {c["date"]} · {c["carrier"]} · {stops} · '
            f'dep {c.get("depart_time","")} → arr {c.get("arrive_time","")}'
            f'{" (" + dur_s + ")" if dur_s else ""}{src}</div>'
            f'<a class="cta" href="{c["link"]}" target="_blank">Open on Google Flights →</a>')
    else:
        hero = '<div class="price">—</div><div class="when">No fares returned this run.</div>'

    # Plain-string (not f-string) so JS braces don't clash with the template.
    refresh_script = (
        "<script>\n"
        "const TRIGGER_URL=" + json.dumps(TRIGGER_URL) + ";\n"
        "async function doRefresh(){\n"
        "  const b=document.getElementById('refreshBtn'),t=document.getElementById('refreshTxt');\n"
        "  b.disabled=true;t.textContent='Starting a fresh check…';\n"
        "  try{\n"
        "    const r=await fetch(TRIGGER_URL,{method:'POST'});\n"
        "    const j=await r.json().catch(()=>({}));\n"
        "    t.textContent=(j&&j.message)?j.message:'Refresh started — new prices in ~1 min.';\n"
        "    setTimeout(()=>location.reload(),75000);\n"
        "  }catch(e){t.textContent='Could not start — try again in a moment.';b.disabled=false;}\n"
        "}\n"
        "</script>"
    )

    html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bali Flight Watch</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#000;color:#fff;font-family:-apple-system,'SF Pro Display',Inter,system-ui,sans-serif;
-webkit-font-smoothing:antialiased;padding:6vh 6vw;max-width:900px;margin:0 auto}}
.eyebrow{{text-transform:uppercase;letter-spacing:.28em;font-size:12px;color:#8a8a8f;font-weight:600}}
h1{{font-size:clamp(30px,5vw,46px);font-weight:800;letter-spacing:-.02em;margin:8px 0 2px}}
.route{{color:#8a8a8f;font-size:16px;margin-bottom:40px}}
.hero{{border:1px solid #1c1c1e;border-radius:22px;padding:34px 32px;margin-bottom:44px;
background:linear-gradient(180deg,#0d0d0f,#000)}}
.hlabel{{text-transform:uppercase;letter-spacing:.2em;font-size:11px;color:#00e0a4;font-weight:700}}
.price{{font-size:clamp(56px,11vw,92px);font-weight:800;letter-spacing:-.04em;line-height:1;margin:10px 0 6px}}
.when{{color:#c7c7cc;font-size:15px;margin-bottom:22px}}
.cta{{display:inline-block;background:#fff;color:#000;text-decoration:none;font-weight:700;
padding:13px 22px;border-radius:999px;font-size:15px}}
.cta:hover{{opacity:.85}}
.refresh{{display:inline-flex;align-items:center;gap:8px;margin-bottom:34px;
border:1px solid #2a2a2c;color:#fff;text-decoration:none;font-weight:600;font-family:inherit;
padding:10px 18px;border-radius:999px;font-size:14px;background:#0d0d0f;cursor:pointer}}
.refresh:hover{{background:#161618;border-color:#3a3a3c}}
.refresh:disabled{{opacity:.6;cursor:default}}
.refresh .dot{{width:7px;height:7px;border-radius:50%;background:#00e0a4;
box-shadow:0 0 8px #00e0a4}}
.gtitle{{font-size:13px;text-transform:uppercase;letter-spacing:.2em;color:#8a8a8f;font-weight:600;margin-bottom:18px}}
.row{{display:flex;align-items:center;gap:16px;text-decoration:none;color:#fff;padding:7px 0}}
.row:hover .track{{background:#161618}}
.row.best .p{{color:#00e0a4}}
.d{{width:96px;font-size:14px;color:#c7c7cc;flex:none}}
.d b{{color:#fff;font-weight:700}}
.track{{flex:1;height:26px;background:#0e0e10;border-radius:7px;overflow:hidden}}
.bar{{display:block;height:100%;background:#3a3a3c;border-radius:7px}}
.bar.best{{background:linear-gradient(90deg,#00e0a4,#00b487)}}
.bar.miss{{background:repeating-linear-gradient(45deg,#1c1c1e,#1c1c1e 6px,#111 6px,#111 12px)}}
.p{{width:64px;text-align:right;font-weight:700;font-size:15px;flex:none;font-variant-numeric:tabular-nums}}
.foot{{margin-top:36px;color:#5a5a5f;font-size:12px;line-height:1.7}}
.foot b{{color:#8a8a8f}}
</style></head><body>
<div class="eyebrow">Bali Flight Watch</div>
<h1>Stockholm → Bali</h1>
<div class="route">One-way · {out["window"]} · scanned twice daily</div>
<button class="refresh" id="refreshBtn" onclick="doRefresh()">
<span class="dot"></span><span id="refreshTxt">Refresh now — run a fresh check</span></button>
<div class="hero"><div class="hlabel">Cheapest right now</div>{hero}</div>
<div class="gtitle">Every departure day in the window</div>
{''.join(bars)}
<div class="foot"><b>Route</b> {out["route"]} &nbsp;·&nbsp; <b>Updated</b> {out["scanned_at"]}<br>
Live Google Flights fares via Apify. Tap any day to open that date on Google Flights.</div>
{refresh_script}
</body></html>"""
    path = os.path.join(HERE, "dashboard.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"Wrote {path}")
    return path


def load_prev_best():
    env = os.environ.get("PREV_BEST")          # set by CI from the last published results
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    try:
        with open(STATE) as f:
            return json.load(f).get("best_price")
    except Exception:
        return None


def save_best(price):
    with open(STATE, "w") as f:
        json.dump({"best_price": price, "at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}, f)


def delta_line(price, prev):
    if prev is None:
        return "First check — tracking from here."
    diff = price - prev
    if diff == 0:
        return f"Same as last check (${prev})."
    arrow = "↓" if diff < 0 else "↑"
    return f"{arrow} ${abs(diff):.0f} vs last check (was ${prev:.0f})."


def send_whatsapp(out):
    c = out["cheapest"]
    if not c:
        msg = ("✈️ Bali Flight Watch\nNo fares returned this run for "
               f"{out['route']} ({out['window']}). Will retry next check.")
    else:
        prev = load_prev_best()
        stops = "nonstop" if c.get("stops") == 0 else f"{c.get('stops')} stop"
        msg = (f"✈️ Cheapest Stockholm → Bali ({out['window']})\n\n"
               f"*${c['price']:.0f}* on *{c['weekday']} {c['date']}*\n"
               f"{c['carrier']}, {stops}, dep {c.get('depart_time','?')}\n"
               f"{delta_line(c['price'], prev)}\n\n"
               f"Book: {c['link']}")
        save_best(c["price"])

    url = "https://api.fonnte.com/send"
    body = {"target": OWNER_WHATSAPP, "message": msg, "countryCode": "0"}
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 method="POST",
                                 headers={"Authorization": FONNTE_TOKEN,
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read().decode())
    print("Fonnte response:", resp)
    print("\n--- message sent ---\n" + msg)
    return resp


def run_once(send=True):
    """Full cycle: scan -> render dashboard -> optionally WhatsApp. Returns results dict."""
    out = scan()
    render_dashboard(out)
    if send and FONNTE_TOKEN and OWNER_WHATSAPP:
        try:
            send_whatsapp(out)
        except Exception as e:
            print("WhatsApp send failed:", e)
    return out


if __name__ == "__main__":
    if "--render-only" in sys.argv:            # rebuild dashboard from existing results.json
        with open(RESULTS) as f:
            render_dashboard(json.load(f))
        sys.exit(0)
    if not APIFY_TOKEN:
        sys.exit("APIFY_TOKEN not set")
    out = scan()
    render_dashboard(out)
    if "--no-whatsapp" in sys.argv:
        print("Skipping WhatsApp (--no-whatsapp).")
    else:
        if not (FONNTE_TOKEN and OWNER_WHATSAPP):
            sys.exit("FONNTE_TOKEN / OWNER_WHATSAPP not set")
        send_whatsapp(out)
