#!/usr/bin/env python3
"""
Bali Flight Watch — scans the cheapest one-way flight from each origin
(Stockholm ARN, Copenhagen CPH) to Bali (DPS) for every departure date in a
window, with airline + baggage read-out, then WhatsApps the cheapest via Fonnte.

Data source: Apify actor makework36/flight-price-scraper (multi-source: Google,
Kiwi, …). No calendar mode, so we fire one bounded async run per (origin, date).

Baggage note: the feed does NOT return live bag prices, so "bag included?" is
inferred from carrier type (full-service vs low-cost / self-transfer) and the
extra-bag fee is a curated estimate — always confirm at booking.

Env vars: APIFY_TOKEN, FONNTE_TOKEN, OWNER_WHATSAPP  (recipient e.g. 1xxxxxxxxxx)
Flags: --no-whatsapp | --render-only
"""
import os, sys, json, time, urllib.request, urllib.error
from datetime import date, timedelta

ORIGINS      = [("ARN", "Stockholm", "🇸🇪"), ("CPH", "Copenhagen", "🇩🇰")]
DESTINATION  = "DPS"
DEST_NAME    = "Bali"
WINDOW_START = date(2026, 10, 1)
WINDOW_END   = date(2026, 10, 15)   # inclusive
ADULTS       = 1
CURRENCY     = "USD"
MAX_FLIGHTS  = 25                   # pull enough that 1-stop options survive the filter
MAX_STOPS    = 1                    # only clean itineraries (<=1 connection)
ACTOR        = "makework36~flight-price-scraper"
RUN_MEM      = 512
CONCURRENCY  = 4
MAX_CHARGE   = "0.05"

HERE        = os.path.dirname(os.path.abspath(__file__))
RESULTS     = os.path.join(HERE, "results.json")
STATE       = os.path.join(HERE, "state.json")
TRIGGER_URL = os.environ.get(
    "TRIGGER_URL",
    "https://github.com/gabrielmendoz/bali-flight-watch/actions/workflows/watch.yml")

APIFY_TOKEN    = os.environ.get("APIFY_TOKEN", "")
FONNTE_TOKEN   = os.environ.get("FONNTE_TOKEN", "")
OWNER_WHATSAPP = os.environ.get("OWNER_WHATSAPP", "")

# ---- Baggage inference (feed has no live bag data) --------------------------
# Full-service carriers whose cheapest long-haul economy fare to Asia normally
# INCLUDES a checked bag (~23–30 kg).
FULL_SERVICE = (
    "qatar", "turkish", "emirates", "etihad", "china eastern", "china southern",
    "air china", "thai", "singapore", "cathay", "malaysia", "vietnam", "eva",
    "asiana", "korean", "japan airlines", "ana", "klm", "air france", "lufthansa",
    "swiss", "austrian", "finnair", "sas", "scandinavian", "oman", "gulf air",
    "saudia", "srilankan", "garuda", "philippine", "hainan", "xiamen", "sichuan",
    "klm royal", "brussels", "lot", "aeroflot",
)
# Low-cost carriers: base fare has NO checked bag. Value = approx first-bag fee (USD).
LCC_FEE = {
    "ryanair": 35, "wizz": 45, "easyjet": 45, "norwegian": 45, "scoot": 55,
    "airasia": 55, "air asia": 55, "jetstar": 55, "cebu": 45, "vietjet": 45,
    "eurowings": 45, "flydubai": 45, "transavia": 40, "smartwings": 45,
    "pegasus": 40, "indigo": 40,
}


def bag_note(airline, self_transfer):
    n = (airline or "").lower()
    for k, fee in LCC_FEE.items():
        if k in n:
            return {"included": False, "extra_fee": fee}
    if any(k in n for k in FULL_SERVICE):
        return {"included": True, "extra_fee": None}
    if self_transfer:
        return {"included": False, "extra_fee": 50}
    return {"included": True, "extra_fee": None}   # default long-haul = full-service


def bag_label(b):
    if b["included"]:
        return "✓ checked bag included"
    return f"✗ no checked bag · add ~${b['extra_fee']}"


# ---- Apify plumbing ---------------------------------------------------------
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


def start_run(origin, dep_date):
    url = (f"https://api.apify.com/v2/acts/{ACTOR}/runs"
           f"?token={APIFY_TOKEN}&timeout=180&memory={RUN_MEM}"
           f"&maxTotalChargeUsd={MAX_CHARGE}")
    body = {"origin": origin, "destination": DESTINATION,
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
        if isinstance(st, int) and st > MAX_STOPS:
            continue
        cands.append(x)
    if not cands:
        return None
    best = min(cands, key=lambda x: x["bestPrice"])
    self_transfer = bool(best.get("isSelfTransfer"))
    b = bag_note(best.get("airline"), self_transfer)
    return {
        "price": best["bestPrice"],
        "currency": best.get("currency", "USD") or "USD",
        "carrier": best.get("airline", "?"),
        "source": best.get("cheapestSource"),
        "stops": best.get("stops"),
        "self_transfer": self_transfer,
        "depart_time": _hhmm(best.get("departTime")),
        "arrive_date": _ymd(best.get("arriveTime")),
        "arrive_time": _hhmm(best.get("arriveTime")),
        "duration_str": best.get("duration"),
        "bag_included": b["included"],
        "bag_extra_fee": b["extra_fee"],
        "bag_label": bag_label(b),
    }


def gflights_link(origin, dep_date):
    return (f"https://www.google.com/travel/flights?q=Flights%20{origin}%20to%20"
            f"{DESTINATION}%20on%20{dep_date}%20oneway")


# ---- Scan (all origins x dates, bounded pool) -------------------------------
def scan():
    tasks = [(o, d.isoformat()) for o in ORIGINS for d in daterange()]  # (origin_tuple, ds)
    print(f"Scanning {len(ORIGINS)} origins x {sum(1 for _ in daterange())} dates "
          f"= {len(tasks)} runs (mem {RUN_MEM}MB, {CONCURRENCY} at a time)")
    queue   = list(tasks)
    active  = {}          # key -> {run_id, ds_id, origin, ds}
    results = {}          # (origin_code, ds) -> cheapest dict or None

    def key(o, ds):
        return (o[0], ds)

    def fill():
        while queue and len(active) < CONCURRENCY:
            o, ds = queue.pop(0)
            try:
                rid, dsid = start_run(o[0], ds)
                active[rid] = {"run_id": rid, "ds_id": dsid, "origin": o, "ds": ds}
            except Exception as e:
                results[key(o, ds)] = None
                print(f"  FAILED start {o[0]} {ds}: {e}")

    fill()
    for _ in range(240):
        if not active and not queue:
            break
        for rid in list(active.keys()):
            try:
                st = run_status(rid)
            except Exception:
                continue
            if st in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                r = active.pop(rid)
                k = key(r["origin"], r["ds"])
                if st == "SUCCEEDED":
                    try:
                        results[k] = cheapest_from(dataset_items(r["ds_id"]))
                        p = results[k]["price"] if results[k] else None
                        print(f"  {r['origin'][0]} {r['ds']}: {'$'+str(p) if p else 'no fares'}")
                    except Exception as e:
                        results[k] = None
                        print(f"  {r['origin'][0]} {r['ds']}: dataset error {e}")
                else:
                    results[k] = None
                    print(f"  {r['origin'][0]} {r['ds']}: run {st}")
        fill()
        if active or queue:
            time.sleep(5)

    routes = []
    for o in ORIGINS:
        code, name, flag = o
        rows = []
        for d in daterange():
            ds = d.isoformat()
            best = results.get((code, ds))
            rows.append({"date": ds, "weekday": d.strftime("%a"),
                         "link": gflights_link(code, ds),
                         **(best or {"price": None})})
        priced = [r for r in rows if r.get("price") is not None]
        cheapest = min(priced, key=lambda r: r["price"]) if priced else None
        routes.append({"origin": code, "origin_name": name, "flag": flag,
                       "route": f"{code} → {DESTINATION}", "rows": rows,
                       "cheapest": cheapest})

    all_cheap = [r["cheapest"] for r in routes if r["cheapest"]]
    overall = min(all_cheap, key=lambda c: c["price"]) if all_cheap else None
    overall_origin = None
    if overall:
        for r in routes:
            if r["cheapest"] is overall:
                overall_origin = r["origin_name"]

    out = {
        "dest_name": DEST_NAME,
        "window": f"{WINDOW_START.isoformat()} to {WINDOW_END.isoformat()}",
        "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "routes": routes,
        "overall_cheapest": overall,
        "overall_origin": overall_origin,
    }
    with open(RESULTS, "w") as f:
        json.dump(out, f, indent=2)
    n_priced = sum(1 for r in routes for x in r["rows"] if x.get("price"))
    n_total = sum(len(r["rows"]) for r in routes)
    print(f"Wrote {RESULTS} ({n_priced}/{n_total} priced)")
    return out


# ---- Dashboard --------------------------------------------------------------
def _route_panel(rt):
    rows = rt["rows"]
    priced = [r for r in rows if r.get("price") is not None]
    lo = min((r["price"] for r in priced), default=0)
    hi = max((r["price"] for r in priced), default=1)
    span = (hi - lo) or 1
    c = rt["cheapest"]

    bars = []
    for r in rows:
        p = r.get("price")
        is_best = c and r["date"] == c["date"]
        if p is None:
            pct, label, cls = 6, "—", "bar miss"
            meta = "no fares this run"
        else:
            pct = 12 + 88 * (1 - (p - lo) / span)
            label = f"${p:.0f}"
            cls = "bar best" if is_best else "bar"
            stops = "nonstop" if r.get("stops") == 0 else f'{r.get("stops")} stop'
            bagcls = "ok" if r.get("bag_included") else "no"
            meta = (f'{r.get("carrier","?")} · {stops} · '
                    f'<span class="bag {bagcls}">{r.get("bag_label","")}</span>')
        d = r["date"][8:10]
        bars.append(
            f'<a class="row{" best" if is_best else ""}" href="{r["link"]}" target="_blank">'
            f'<div class="rtop"><span class="d"><b>{r["weekday"]}</b> {d} Oct</span>'
            f'<span class="track"><span class="{cls}" style="width:{pct:.1f}%"></span></span>'
            f'<span class="p">{label}</span></div>'
            f'<div class="meta">{meta}</div></a>')

    if c:
        stops = "nonstop" if c.get("stops") == 0 else f'{c.get("stops")} stop'
        dur_s = c.get("duration_str") or ""
        src = f' · via {c.get("source")}' if c.get("source") else ""
        bagcls = "ok" if c.get("bag_included") else "no"
        hero = (
            f'<div class="price">${c["price"]:.0f}</div>'
            f'<div class="when">{c["weekday"]} {c["date"]} · {c["carrier"]} · {stops} · '
            f'dep {c.get("depart_time","")} → arr {c.get("arrive_time","")}'
            f'{" (" + dur_s + ")" if dur_s else ""}{src}</div>'
            f'<div class="when"><span class="bag {bagcls}">{c.get("bag_label","")}</span></div>'
            f'<a class="cta" href="{c["link"]}" target="_blank">Open on Google Flights →</a>')
    else:
        hero = '<div class="price">—</div><div class="when">No fares returned this run.</div>'

    return (f'<div class="panel"><div class="ptitle">{rt["flag"]} {rt["origin_name"]} '
            f'<span class="pcode">{rt["route"]}</span></div>'
            f'<div class="hero"><div class="hlabel">Cheapest right now</div>{hero}</div>'
            f'<div class="gtitle">Every departure day</div>{"".join(bars)}</div>')


def render_dashboard(out):
    panels = "".join(_route_panel(rt) for rt in out["routes"])
    oc = out.get("overall_cheapest")
    if oc:
        best_line = (f'Cheapest overall: <b>${oc["price"]:.0f}</b> from '
                     f'{out.get("overall_origin","")} on {oc["weekday"]} {oc["date"]}')
    else:
        best_line = "No fares returned this run."

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
-webkit-font-smoothing:antialiased;padding:6vh 6vw;max-width:1180px;margin:0 auto}}
.eyebrow{{text-transform:uppercase;letter-spacing:.28em;font-size:12px;color:#8a8a8f;font-weight:600}}
h1{{font-size:clamp(30px,5vw,46px);font-weight:800;letter-spacing:-.02em;margin:8px 0 2px}}
.route{{color:#8a8a8f;font-size:16px;margin-bottom:18px}}
.bestbar{{color:#c7c7cc;font-size:15px;margin-bottom:22px}}
.bestbar b{{color:#00e0a4}}
.refresh{{display:inline-flex;align-items:center;gap:8px;margin-bottom:38px;
border:1px solid #2a2a2c;color:#fff;text-decoration:none;font-weight:600;font-family:inherit;
padding:10px 18px;border-radius:999px;font-size:14px;background:#0d0d0f;cursor:pointer}}
.refresh:hover{{background:#161618;border-color:#3a3a3c}}
.refresh:disabled{{opacity:.6;cursor:default}}
.refresh .dot{{width:7px;height:7px;border-radius:50%;background:#00e0a4;box-shadow:0 0 8px #00e0a4}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:26px}}
.panel{{min-width:0}}
.ptitle{{font-size:20px;font-weight:800;letter-spacing:-.01em;margin-bottom:14px}}
.pcode{{color:#5a5a5f;font-weight:600;font-size:14px;margin-left:6px}}
.hero{{border:1px solid #1c1c1e;border-radius:20px;padding:26px 24px;margin-bottom:26px;
background:linear-gradient(180deg,#0d0d0f,#000)}}
.hlabel{{text-transform:uppercase;letter-spacing:.2em;font-size:11px;color:#00e0a4;font-weight:700}}
.price{{font-size:clamp(46px,8vw,68px);font-weight:800;letter-spacing:-.04em;line-height:1;margin:8px 0 6px}}
.when{{color:#c7c7cc;font-size:14px;margin-bottom:6px}}
.cta{{display:inline-block;margin-top:14px;background:#fff;color:#000;text-decoration:none;font-weight:700;
padding:11px 20px;border-radius:999px;font-size:14px}}
.cta:hover{{opacity:.85}}
.gtitle{{font-size:12px;text-transform:uppercase;letter-spacing:.2em;color:#8a8a8f;font-weight:600;margin-bottom:12px}}
.row{{display:block;text-decoration:none;color:#fff;padding:8px 0;border-bottom:1px solid #0e0e10}}
.row:hover .track{{background:#161618}}
.row.best .p{{color:#00e0a4}}
.rtop{{display:flex;align-items:center;gap:12px}}
.d{{width:82px;font-size:13px;color:#c7c7cc;flex:none}}
.d b{{color:#fff;font-weight:700}}
.track{{flex:1;height:22px;background:#0e0e10;border-radius:6px;overflow:hidden}}
.bar{{display:block;height:100%;background:#3a3a3c;border-radius:6px}}
.bar.best{{background:linear-gradient(90deg,#00e0a4,#00b487)}}
.bar.miss{{background:repeating-linear-gradient(45deg,#1c1c1e,#1c1c1e 6px,#111 6px,#111 12px)}}
.p{{width:56px;text-align:right;font-weight:700;font-size:14px;flex:none;font-variant-numeric:tabular-nums}}
.meta{{margin:4px 0 0 94px;font-size:12px;color:#8a8a8f}}
.bag.ok{{color:#00e0a4}}
.bag.no{{color:#ff9f43}}
.foot{{margin-top:40px;color:#5a5a5f;font-size:12px;line-height:1.7}}
.foot b{{color:#8a8a8f}}
</style></head><body>
<div class="eyebrow">Bali Flight Watch</div>
<h1>Stockholm & Copenhagen → Bali</h1>
<div class="route">One-way · {out["window"]} · max 1 stop · scanned twice daily</div>
<div class="bestbar">{best_line}</div>
<button class="refresh" id="refreshBtn" onclick="doRefresh()">
<span class="dot"></span><span id="refreshTxt">Refresh now — run a fresh check</span></button>
<div class="grid">{panels}</div>
<div class="foot"><b>Updated</b> {out["scanned_at"]} &nbsp;·&nbsp; Live fares via Apify (Google/Kiwi).
Tap any day to open it on Google Flights.<br>
<b>Baggage</b> is inferred from carrier type — full-service carriers normally include a checked bag,
low-cost / self-transfer fares don't. Extra-bag fees are approximate; confirm exact allowance at booking.</div>
{refresh_script}
</body></html>"""
    path = os.path.join(HERE, "dashboard.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"Wrote {path}")
    return path


# ---- State + WhatsApp -------------------------------------------------------
def load_prev_best():
    env = os.environ.get("PREV_BEST")
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
        return f"Same as last check (${prev:.0f})."
    arrow = "↓" if diff < 0 else "↑"
    return f"{arrow} ${abs(diff):.0f} vs last check (was ${prev:.0f})."


def _origin_line(rt):
    c = rt["cheapest"]
    if not c:
        return f'{rt["flag"]} {rt["origin_name"]}: no fares this run'
    stops = "nonstop" if c.get("stops") == 0 else f'{c.get("stops")} stop'
    bag = "✓ bag incl" if c.get("bag_included") else f"✗ no bag (+~${c.get('bag_extra_fee')})"
    return (f'{rt["flag"]} {rt["origin_name"]}: *${c["price"]:.0f}* {c["weekday"]} {c["date"]}\n'
            f'   {c["carrier"]}, {stops}, {bag}')


def send_whatsapp(out):
    oc = out.get("overall_cheapest")
    lines = [f'✈️ Cheapest to {out["dest_name"]} ({out["window"]})', ""]
    for rt in out["routes"]:
        lines.append(_origin_line(rt))
    if oc:
        prev = load_prev_best()
        lines += ["", f'Cheapest overall: *${oc["price"]:.0f}* from {out.get("overall_origin","")}',
                  delta_line(oc["price"], prev), "", f'Book: {oc["link"]}']
        save_best(oc["price"])
    msg = "\n".join(lines)

    url = "https://api.fonnte.com/send"
    body = {"target": OWNER_WHATSAPP, "message": msg, "countryCode": "0"}
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Authorization": FONNTE_TOKEN,
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read().decode())
    print("Fonnte response:", resp)
    print("\n--- message ---\n" + msg)
    return resp


def run_once(send=True):
    out = scan()
    render_dashboard(out)
    if send and FONNTE_TOKEN and OWNER_WHATSAPP:
        try:
            send_whatsapp(out)
        except Exception as e:
            print("WhatsApp send failed:", e)
    return out


if __name__ == "__main__":
    if "--render-only" in sys.argv:
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
