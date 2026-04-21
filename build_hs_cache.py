"""
Builds hubspot_cache.json from the Apps Script JSON endpoint.
Run this script to refresh the static fallback cache.
The live dashboard auto-fetches from Apps Script every 15 min,
but this file is used as fallback if the live fetch fails.
"""
import json, datetime, urllib.parse
from pathlib import Path

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

BASE = Path(__file__).parent

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxVmtRyJiFex9yQMPDsk6ivMY3f6clHHe0mdEBnxMxiP02yWoaBzGTmv5-8yxopexOs/exec"
MB_DATE_COL    = 'Date entered "Meeting Scheduled (Placements — Inbound Sales Stage)"'
MH_COL         = "Initial Meeting Outcome"
EL_SENT_COL    = "Date Engagement Letter Was Sent"
EL_SIGNED_COL  = 'Date entered "Closed Won (Placements — Inbound Sales Stage)"'
PLACEHOLDERS   = {"{{campaign.name}}", "{{ad.name}}", ""}


def clean_utm(val):
    if not val:
        return ""
    try:
        val = urllib.parse.unquote_plus(str(val))
    except Exception:
        pass
    val = val.strip()
    return "" if val in PLACEHOLDERS else val


def parse_date(val):
    """Convert any HubSpot date string to YYYY-MM-DD."""
    import datetime as _dt
    val = str(val or "").strip()
    if not val or val == "(No value)":
        return ""
    if len(val) >= 10 and val[4] == "-" and val[7] == "-":
        return val[:10]
    try:
        return _dt.datetime.strptime(val[:24], "%a %b %d %Y %H:%M:%S").strftime("%Y-%m-%d")
    except Exception:
        return val[:10] if len(val) >= 10 else ""


def truthy(v):
    s = str(v or "").strip()
    return bool(s) and s != "(No value)"


print("Fetching from Apps Script…")
r = httpx.get(APPS_SCRIPT_URL, follow_redirects=True, timeout=30)
rows = r.json()
print(f"Received {len(rows)} rows")

contacts = []
for row in rows:
    raw_date = (str(row.get(MB_DATE_COL, "") or "").strip()
                or str(row.get("Create Date", "") or "").strip())
    date = parse_date(raw_date)
    if not date or len(date) < 10:
        continue

    email       = str(row.get("Email", "") or "").strip().lower()
    mql_val     = str(row.get("MQL", "") or "").strip()
    sql_val     = str(row.get("SQL", "") or "").strip()
    attribution = str(row.get("Attribution (Contact-Level)", "") or "").strip() or "(unknown)"

    mh_val  = str(row.get(MH_COL, "") or "").strip()
    mh      = bool(mh_val) and mh_val not in {"(No value)", "No Show"}
    el_sent  = truthy(row.get(EL_SENT_COL, ""))
    el_signed = truthy(row.get(EL_SIGNED_COL, ""))

    contacts.append({
        "date":         date,
        "email":        email,
        "mql":          mql_val == "Yes",
        "sql":          sql_val == "Yes",
        "mh":           mh,
        "el_sent":      el_sent,
        "el_signed":    el_signed,
        "attribution":  attribution,
        "utm_source":   clean_utm(row.get("utm_source",   "")) or "",
        "utm_campaign": clean_utm(row.get("utm_campaign", "")) or "(no utm_campaign)",
        "utm_content":  clean_utm(row.get("utm_content",  "")) or "(no utm_content)",
    })

contacts.sort(key=lambda x: x["date"])

dates    = [c["date"] for c in contacts]
date_min = dates[0]  if dates else "2026-01-01"
date_max = dates[-1] if dates else "2026-12-31"

cache = {
    "contacts":     contacts,
    "deals":        [c for c in contacts if c["mql"]],
    "date_min":     date_min,
    "date_max":     date_max,
    "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
}

out_path = BASE / "hubspot_cache.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(cache, f, indent=2, ensure_ascii=False)

mql_all      = [c for c in contacts if c["mql"]]
mh_all       = [c for c in contacts if c["mh"]]
sql_all      = [c for c in contacts if c["sql"]]
el_sent_all  = [c for c in contacts if c["el_sent"]]
el_signed_all= [c for c in contacts if c["el_signed"]]
print(f"Cache written → {out_path}")
print(f"Total MB: {len(contacts)}, range: {date_min} → {date_max}")
print(f"MQL: {len(mql_all)}  MH: {len(mh_all)}  SQL: {len(sql_all)}  EL Sent: {len(el_sent_all)}  EL Signed: {len(el_signed_all)}")

for year in ["2024", "2025", "2026"]:
    yr          = [c for c in contacts if c["date"].startswith(year)]
    yr_mql      = [c for c in yr if c["mql"]]
    yr_mh       = [c for c in yr if c["mh"]]
    yr_sql      = [c for c in yr if c["sql"]]
    yr_el_sent  = [c for c in yr if c["el_sent"]]
    yr_el_signed= [c for c in yr if c["el_signed"]]
    if yr:
        print(f"  {year}: mb={len(yr)} mql={len(yr_mql)} mh={len(yr_mh)} sql={len(yr_sql)} el_sent={len(yr_el_sent)} el_signed={len(yr_el_signed)}")
