"""
Builds hubspot_cache.json from the MB Google Sheet tab.

  MB sheet (gid=1408761440): All contacts who booked a meeting.

Date logic:
  MB date  → "Date entered 'Meeting Scheduled (Placements — Inbound Sales Stage)'"
             fallback: "Create Date"
  MH flag  → "Meeting Start Time" column is non-empty (meeting actually happened)

Funnel order: MB → MQL → MH → SQL

To refresh: just re-run this script. It downloads the sheet live.
"""
import csv, io, json, datetime, urllib.request, urllib.parse
from pathlib import Path

BASE = Path(__file__).parent

SHEET_BASE = "https://docs.google.com/spreadsheets/d/1szR5aHU5j1FijE4mBVmlx2A0AsA7-lvocgsbO6UFmCw/export?format=csv&gid="
GID_MB = "1408761440"

PLACEHOLDERS = {"{{campaign.name}}", "{{ad.name}}", ""}

MB_DATE_COL    = "Date entered \"Meeting Scheduled (Placements — Inbound Sales Stage)\""
MH_DATE_COL    = "Meeting Start Time"
EL_SENT_COL    = "Date Engagement Letter Was Sent"


def clean_utm(val: str) -> str:
    if not val:
        return ""
    try:
        val = urllib.parse.unquote_plus(val)
    except Exception:
        pass
    val = val.strip()
    if val in PLACEHOLDERS:
        return ""
    return val


# ── Download MB sheet ─────────────────────────────────────────────────────────
print("Downloading MB sheet…")
with urllib.request.urlopen(SHEET_BASE + GID_MB) as resp:
    mb_content = resp.read().decode("utf-8")

contacts = []
reader = csv.DictReader(io.StringIO(mb_content))

# Debug: show available columns on first run
cols = reader.fieldnames or []
mb_date_found  = MB_DATE_COL in cols
mh_col_found   = MH_DATE_COL in cols
el_sent_found  = EL_SENT_COL in cols
print(f"MB date column found  : {mb_date_found}  ({MB_DATE_COL!r})")
print(f"MH date column found  : {mh_col_found}  ({MH_DATE_COL!r})")
print(f"EL Sent column found  : {el_sent_found}  ({EL_SENT_COL!r})")

for row in reader:
    # MB date: stage-specific date → fallback Create Date
    raw_date = (row.get(MB_DATE_COL, "") or row.get("Create Date", "")).strip()
    date = raw_date[:10]  # YYYY-MM-DD
    if not date or len(date) < 10:
        continue

    email      = row.get("Email", "").strip().lower()
    mql_val    = row.get("MQL", "").strip()
    sql_val    = row.get("SQL", "").strip()
    attribution = row.get("Attribution (Contact-Level)", "").strip() or "(unknown)"

    # MH: meeting actually happened if Meeting Start Time is non-empty
    mh_raw  = row.get(MH_DATE_COL, "").strip()
    mh      = bool(mh_raw)

    # EL Sent: engagement letter was sent if date column is non-empty
    el_sent = bool(row.get(EL_SENT_COL, "").strip())

    contact = {
        "date":         date,
        "email":        email,
        "mql":          mql_val == "Yes",
        "sql":          sql_val == "Yes",
        "mh":           mh,
        "el_sent":      el_sent,
        "attribution":  attribution,
        "utm_source":   clean_utm(row.get("utm_source",   "")) or "",
        "utm_campaign": clean_utm(row.get("utm_campaign", "")) or "(no utm_campaign)",
        "utm_content":  clean_utm(row.get("utm_content",  "")) or "(no utm_content)",
    }
    contacts.append(contact)

contacts.sort(key=lambda x: x["date"])

dates    = [c["date"] for c in contacts]
date_min = dates[0]  if dates else "2026-01-01"
date_max = dates[-1] if dates else "2026-12-31"

cache = {
    "contacts":     contacts,
    "deals":        [c for c in contacts if c["mql"]],   # backward-compat key
    "date_min":     date_min,
    "date_max":     date_max,
    "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
}

out_path = BASE / "hubspot_cache.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(cache, f, indent=2, ensure_ascii=False)

# ── Summary ───────────────────────────────────────────────────────────────────
mql_all     = [c for c in contacts if c["mql"]]
sql_all     = [c for c in contacts if c["sql"]]
mh_all      = [c for c in contacts if c["mh"]]
el_sent_all = [c for c in contacts if c["el_sent"]]
print(f"Cache written → {out_path}")
print(f"Total MB: {len(contacts)}, range: {date_min} → {date_max}")
print(f"MQL: {len(mql_all)}  MH: {len(mh_all)}  SQL: {len(sql_all)}  EL Sent: {len(el_sent_all)}")

for year in ["2024", "2025", "2026"]:
    yr     = [c for c in contacts if c["date"].startswith(year)]
    yr_mql     = [c for c in yr if c["mql"]]
    yr_mh      = [c for c in yr if c["mh"]]
    yr_sql     = [c for c in yr if c["sql"]]
    yr_el_sent = [c for c in yr if c["el_sent"]]
    if yr:
        print(f"  {year}: mb={len(yr)} mql={len(yr_mql)} mh={len(yr_mh)} sql={len(yr_sql)} el_sent={len(yr_el_sent)}")
