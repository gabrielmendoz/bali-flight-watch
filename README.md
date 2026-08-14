# Bali Flight Watch

Watches the cheapest one-way **Stockholm (ARN) → Bali (DPS)** flight across a date
window (default **Oct 1–15, 2026**, max 1 stop). Runs twice daily in the cloud (free),
sends the cheapest to WhatsApp, and publishes a live dashboard.

## How it works
- **Data:** Apify actor `makework36/flight-price-scraper` (multi-source: Google, Kiwi, …),
  one bounded async run per date, cheapest ≤1-stop fare kept. ~cents/month.
- **Notify:** Fonnte WhatsApp API → `OWNER_WHATSAPP`.
- **Dashboard:** self-contained `dashboard.html`, regenerated every run, published to GitHub Pages.
- **Schedule:** GitHub Actions cron at 06:00 & 18:00 UTC (08:00 & 20:00 Sweden). Free.
- **Price delta:** each run reads the previous cheapest from the published `results.json`
  (no server/state needed).

## Files
- `scan.py` — scanner + dashboard renderer + WhatsApp. Stdlib only (no pip installs).
  - `python3 scan.py` — one sweep + WhatsApp
  - `python3 scan.py --no-whatsapp` — sweep + dashboard only
  - `python3 scan.py --render-only` — rebuild dashboard from existing `results.json`
- `.github/workflows/watch.yml` — the twice-daily cloud job + Pages publish.

## Config (edit top of `scan.py`)
`ORIGIN`, `DESTINATION`, `WINDOW_START`, `WINDOW_END`, `MAX_STOPS`, `ADULTS`, `CURRENCY`.

## Secrets (GitHub → repo Settings → Secrets and variables → Actions)
Secrets: `APIFY_TOKEN`, `FONNTE_TOKEN`, `OWNER_WHATSAPP`
Variable: `PAGES_URL` (the published Pages URL, so runs can read the previous price)

## Change the trip
Edit the constants in `scan.py`, commit, push. Next scheduled run (or a manual
**Actions → Bali Flight Watch → Run workflow**) picks it up.
