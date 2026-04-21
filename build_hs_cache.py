"""
Builds hubspot_cache.json from two Google Sheets tabs:

  MB sheet (gid=1408761440): All contacts who booked a meeting (source of truth for MB, MQL, SQL)
  MH sheet (gid=239976551):  Contacts whose meeting was held (used to flag mh=True on MB contacts)

Funnel order: MB → MQL → MH → SQL

To refresh: just re-run this script. It downloads both sheets live.
"""
import csv, io, json, datetime, urllib.request, urllib.parse
from pathlib import Path

BASE = Path(__file__).parent

SHEET_BASE = "https://docs.google.com/spreadsheets/d/1szR5aHU5j1FijE4mBVmlx2A0AsA7-lvocgsbO6UFmCw/export?format=csv&gid="
GID_MB = "1408761440"
GID_MH = "239976551"

PLACEHOLDERS = {"{{campaign.name}}", "{{ad.name}}", ""}


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


# ── Download MH sheet → build email set ──────────────────────────────────────
print("Downloading MH sheet…")
with urllib.request.urlopen(SHEET_BASE + GID_MH) as resp:
    mh_content = resp.read().decode("utf-8")

mh_emails: set = set()
mh_reader = csv.DictReader(io.StringIO(mh_content))
for row in mh_reader:
    email = row.get("Email", "").strip().lower()
    if email:
        mh_emails.add(email)

print(f"MH emails loaded: {len(mh_emails)}")

# ── Download MB sheet → main contacts ────────────────────────────────────────
print("Downloading MB sheet…")
with urllib.request.urlopen(SHEET_BASE + GID_MB) as resp:
    mb_content = resp.read().decode("utf-8")

contacts = []
reader = csv.DictReader(io.StringIO(mb_content))
for row in reader:
    raw_date = row.get("Create Date", "").strip()
    date = raw_date[:10]  # YYYY-MM-DD
    if not date or len(date) < 10:
        continue

    email = row.get("Email", "").strip().lower()
    mql_val = row.get("MQL", "").strip()
    sql_val = row.get("SQL", "").strip()
    attribution = row.get("Attribution (Contact-Level)", "").strip() or "(unknown)"

    contact = {
        "date":         date,
        "email":        email,
        "mql":          mql_val == "Yes",
        "sql":          sql_val == "Yes",
        "mh":           email in mh_emails,
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
mql_all = [c for c in contacts if c["mql"]]
sql_all = [c for c in contacts if c["sql"]]
mh_all  = [c for c in contacts if c["mh"]]
print(f"Cache written → {out_path}")
print(f"Total MB: {len(contacts)}, range: {date_min} → {date_max}")
print(f"MQL: {len(mql_all)}  MH: {len(mh_all)}  SQL: {len(sql_all)}")

for year in ["2024", "2025", "2026"]:
    yr     = [c for c in contacts if c["date"].startswith(year)]
    yr_mql = [c for c in yr if c["mql"]]
    yr_mh  = [c for c in yr if c["mh"]]
    yr_sql = [c for c in yr if c["sql"]]
    if yr:
        print(f"  {year}: mb={len(yr)} mql={len(yr_mql)} mh={len(yr_mh)} sql={len(yr_sql)}")
