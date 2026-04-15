"""
Builds hubspot_cache.json from HubSpot segment contacts.

Architecture: contact-centric (one record per unique contact).
Each contact = one booked meeting. Classification comes from their actual
associated deal stage in HubSpot pipeline 793577095.

Last refreshed: 2026-04-15 via Claude MCP (HubSpot).
To refresh: export the "Meetings Booked" segment from HubSpot, paste contacts
into SEGMENT_CONTACTS below, fetch deal stages via MCP, and re-run this script.
"""
import json
import datetime
import urllib.parse
from pathlib import Path

BASE = Path(__file__).parent

# ---------------------------------------------------------------------------
# Normalise UTM values (URL-decode, strip placeholders)
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Segment contacts — source of truth
# Each entry: contact create date (YYYY-MM-DD), classification
# (held / no_show / scheduled), utm_campaign, utm_content.
#
# Classifications are derived from actual HubSpot deal associations
# (pipeline 793577095 — "Placements — Inbound Sales Stage").
#
# Stage mapping used:
#   scheduled  → stage 1162444910  (future meeting)
#   no_show    → stage 1162732907
#   held       → stages 1162444911 / 1162444912 / 1162444913 / 1162785708 / 1203929600
# ---------------------------------------------------------------------------
SEGMENT_CONTACTS = [
    # ── Pre-April (older contacts with April deals) ─────────────────────────
    {"date": "2024-10-24", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-03-06", "classification": "held",      "utm_campaign": "[MARKETING] [SCHEDULES] [76] [MARKETING-TEAMS]",        "utm_content": ""},
    {"date": "2026-03-29", "classification": "held",      "utm_campaign": "[MARKETING] [INSTANT FORMS] [76] [MARKETING-TEAMS]",    "utm_content": ""},
    # ── April 1 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-01", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-01", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    # ── April 2 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-02", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-02", "classification": "held",      "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-04-02", "classification": "scheduled", "utm_campaign": "[MARKETING] [INSTANT FORMS] [76] [MARKETING-TEAMS]",    "utm_content": ""},
    {"date": "2026-04-02", "classification": "held",      "utm_campaign": "[MARKETING] [INSTANT FORMS] [76] [MARKETING-TEAMS]",    "utm_content": ""},
    {"date": "2026-04-02", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-02", "classification": "held",      "utm_campaign": "",                                                      "utm_content": "link_in_bio"},
    # ── April 3 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-03", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    # ── April 4 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-04", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-04-04", "classification": "scheduled", "utm_campaign": "76_Leads_Instant_Form_Lead_Event_Marketing_Teams",       "utm_content": ""},
    {"date": "2026-04-04", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Jerrica - #02 - scalearmy.com/social-media-manager"},
    {"date": "2026-04-04", "classification": "no_show",   "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-04-04", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    # ── April 5 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-05", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    # ── April 6 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-06", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-06", "classification": "held",      "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Generic Nate - #01"},
    # ── April 7 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-07", "classification": "held",      "utm_campaign": "76_Leads_Instant_Form_Lead_Event_Marketing_Teams",       "utm_content": ""},
    {"date": "2026-04-07", "classification": "held",      "utm_campaign": "",                                                      "utm_content": ""},
    # ── April 8 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-08", "classification": "scheduled", "utm_campaign": "76_Leads_Instant_Form_Lead_Event_Marketing_Teams",       "utm_content": ""},
    {"date": "2026-04-08", "classification": "held",      "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-04-08", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    # ── April 9 ─────────────────────────────────────────────────────────────
    {"date": "2026-04-09", "classification": "no_show",   "utm_campaign": "[MARKETING] [SCHEDULES] [76] [MARKETING-TEAMS]",        "utm_content": "[GENERIC 2 - JANUARY 26]"},
    {"date": "2026-04-09", "classification": "scheduled", "utm_campaign": "81_Leads_Landing_Page_Schedule_Event_Marketing_Teams",   "utm_content": "AD01_IMG_Wasting_money_Ops_managers"},
    {"date": "2026-04-09", "classification": "held",      "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-09", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    # ── April 10 ────────────────────────────────────────────────────────────
    {"date": "2026-04-10", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-10", "classification": "held",      "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-10", "classification": "held",      "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Generic Nate - #01"},
    {"date": "2026-04-10", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    # ── April 11 ────────────────────────────────────────────────────────────
    {"date": "2026-04-11", "classification": "no_show",   "utm_campaign": "81_Leads_Landing_Page_Schedule_Event_Marketing_Teams",   "utm_content": "AD01_IMG_Wasting_money_Ops_managers"},
    {"date": "2026-04-11", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Generic Nate - #01"},
    # ── April 12 ────────────────────────────────────────────────────────────
    {"date": "2026-04-12", "classification": "scheduled", "utm_campaign": "76_Leads_Instant_Form_Lead_Event_Marketing_Teams",       "utm_content": ""},
    {"date": "2026-04-12", "classification": "held",      "utm_campaign": "81_Leads_Landing_Page_Schedule_Event_Marketing_Teams",   "utm_content": "AD01_IMG_Wasting_money_Ops_managers — Cópia"},
    {"date": "2026-04-12", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    # ── April 13 ────────────────────────────────────────────────────────────
    {"date": "2026-04-13", "classification": "scheduled", "utm_campaign": "",                                                      "utm_content": ""},
    {"date": "2026-04-13", "classification": "held",      "utm_campaign": "76_Leads_Instant_Form_Lead_Event_Marketing_Teams",       "utm_content": "AD02_VID_Jerrica_80k_year_SMM"},
    {"date": "2026-04-13", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-04-13", "classification": "scheduled", "utm_campaign": "81_Leads_Landing_Page_Schedule_Event_Marketing_Teams",   "utm_content": "AD01_IMG_Wasting_money_Ops_managers — Cópia"},
    {"date": "2026-04-13", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    # ── April 14 ────────────────────────────────────────────────────────────
    {"date": "2026-04-14", "classification": "scheduled", "utm_campaign": "81_Leads_Landing_Page_Schedule_Event_Marketing_Teams",   "utm_content": "AD01_IMG_Wasting_money_Ops_managers — Cópia"},
    {"date": "2026-04-14", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "OM - #05 - Copy"},
    {"date": "2026-04-14", "classification": "held",      "utm_campaign": "82_Leads_Landing_Page_Schedule_Event_AI_Automation_Specialist", "utm_content": "AD133_VID_Friend_of_my_told_me"},
    # ── April 15 ────────────────────────────────────────────────────────────
    {"date": "2026-04-15", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Generic Nate - #01"},
    {"date": "2026-04-15", "classification": "scheduled", "utm_campaign": "82_Leads_Landing_Page_Schedule_Event_AI_Automation_Specialist", "utm_content": "AD135_VID_Founder_friend_told_me"},
    {"date": "2026-04-15", "classification": "scheduled", "utm_campaign": "[MARKETING] [SCHEDULES] [79]",                          "utm_content": "Generic Nate - #01"},
]

# ---------------------------------------------------------------------------
# Build cache records: normalise UTMs, sort by date
# ---------------------------------------------------------------------------
records = []
for c in SEGMENT_CONTACTS:
    records.append({
        "date":           c["date"],
        "classification": c["classification"],
        "utm_campaign":   clean_utm(c["utm_campaign"]) or "(no utm_campaign)",
        "utm_content":    clean_utm(c["utm_content"])  or "(no utm_content)",
    })

records.sort(key=lambda x: x["date"])

date_min = records[0]["date"]  if records else "2024-10-24"
date_max = records[-1]["date"] if records else "2026-04-15"

cache = {
    "deals":        records,   # key kept as "deals" for API compatibility
    "date_min":     date_min,
    "date_max":     date_max,
    "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
}

out_path = BASE / "hubspot_cache.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(cache, f, indent=2, ensure_ascii=False)

print(f"Cache written → {out_path}")
print(f"Records: {len(records)}, range: {date_min} → {date_max}")

from collections import Counter
cls = Counter(r["classification"] for r in records)
apr = [r for r in records if "2026-04-01" <= r["date"] <= "2026-04-15"]
apr_cls = Counter(r["classification"] for r in apr)
print(f"All-time: booked={len(records)}  {dict(cls)}")
print(f"April 1–15: booked={len(apr)}  {dict(apr_cls)}")
