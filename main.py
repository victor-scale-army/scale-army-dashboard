import os
import io
import csv
import json
import re
import time
import asyncio
import httpx
import urllib.request
import urllib.parse
from datetime import date as _date, timedelta
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

load_dotenv()

BASE_DIR = Path(__file__).parent
META_GRAPH_BASE    = "https://graph.facebook.com/v21.0"

def get_token() -> str:
    return os.getenv("META_ACCESS_TOKEN", "")

def get_accounts() -> list[str]:
    raw = os.getenv("META_ACCOUNT_IDS", "")
    ids = [a.strip() for a in raw.split(",") if a.strip()]
    return [a if a.startswith("act_") else f"act_{a}" for a in ids]

def get_sched_exclude() -> set:
    raw = os.getenv("SCHED_EXCLUDE_CAMPAIGNS", "")
    return {c.strip() for c in raw.split(",") if c.strip()}

def get_hidden_campaigns() -> set:
    raw = os.getenv("HIDDEN_CAMPAIGNS", "")
    return {c.strip() for c in raw.split(",") if c.strip()}

_LEAD_FORM_TYPES  = ("onsite_conversion.lead_grouped",)
_LEAD_WEB_TYPES   = ("offsite_conversion.fb_pixel_lead",)
_PURCHASE_TYPES   = ("purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase")

# Custom conversion cache: populated at first request
_cv_sched_types: tuple = ()
_cv_lead_web_types: tuple = ()
_cv_loaded: bool = False
# LP cache: ad_id → lp url, populated lazily by api_lp / api_creatives
_ad_lp_cache: dict = {}
_INS_FIELDS = "spend,impressions,clicks,reach,actions,video_play_actions"


# ── Period helpers ────────────────────────────────────────────────────────────

def _compute_period(preset: str, since: str, until: str):
    today = _date.today()
    if since and until:
        ds = _date.fromisoformat(since)
        du = _date.fromisoformat(until)
        delta = (du - ds).days + 1
        pu = ds - timedelta(days=1)
        ps = pu - timedelta(days=delta - 1)
        return since, until, ps.isoformat(), pu.isoformat()
    p = preset or "last_7d"
    if p == "today":
        s = u = today
        ps2 = pu2 = today - timedelta(days=1)
    elif p == "yesterday":
        s = u = today - timedelta(days=1)
        ps2 = pu2 = today - timedelta(days=2)
    elif p == "last_7d":
        s, u = today - timedelta(days=6), today
        ps2, pu2 = today - timedelta(days=13), today - timedelta(days=7)
    elif p == "last_30d":
        s, u = today - timedelta(days=29), today
        ps2, pu2 = today - timedelta(days=59), today - timedelta(days=30)
    elif p == "this_month":
        s = today.replace(day=1); u = today
        delta = (u - s).days + 1
        pu2 = s - timedelta(days=1); ps2 = pu2 - timedelta(days=delta - 1)
    elif p == "last_month":
        first_this = today.replace(day=1)
        u = first_this - timedelta(days=1); s = u.replace(day=1)
        pu2 = s - timedelta(days=1); ps2 = pu2.replace(day=1)
    elif p == "all_time":
        s = _date(2000, 1, 1); u = today
        ps2 = _date(1999, 1, 1); pu2 = _date(1999, 12, 31)
    else:
        s, u = today - timedelta(days=6), today
        ps2, pu2 = today - timedelta(days=13), today - timedelta(days=7)
    return s.isoformat(), u.isoformat(), ps2.isoformat(), pu2.isoformat()


# ── Meta API helpers ──────────────────────────────────────────────────────────

def _extract_action(actions: list, types: tuple) -> int:
    return sum(int(float(a.get("value", 0))) for a in (actions or []) if a.get("action_type") in types)

async def _meta_get_all(account_id: str, token: str, params: dict) -> list:
    items = []
    async with httpx.AsyncClient() as c:
        url = f"{META_GRAPH_BASE}/{account_id}/insights"
        qp = {"access_token": token, "limit": 100, **params}
        while url:
            try:
                r = await c.get(url, params=qp, timeout=25)
                if r.status_code != 200:
                    print(f"[meta] {r.status_code}: {r.text[:200]}")
                    break
                body = r.json()
                items.extend(body.get("data", []))
                url = body.get("paging", {}).get("next")
                qp = {}
            except Exception as e:
                print(f"[meta] erro: {e}"); break
    return items

_ATTR_WINDOWS = json.dumps(["7d_click", "1d_view"])

async def _get_insights(account_id: str, token: str, since: str, until: str, level: str) -> list:
    name_map = {"campaign": "campaign_name", "adset": "adset_name", "ad": "ad_name"}
    name_f   = name_map.get(level, "")
    extra    = (",campaign_name" if level == "adset"
                else ",campaign_name,adset_name" if level == "ad"
                else "")
    fields = f"{name_f}{extra},{_INS_FIELDS}" if name_f else _INS_FIELDS
    return await _meta_get_all(account_id, token, {
        "fields": fields,
        "time_range": json.dumps({"since": since, "until": until}),
        "level": level,
        "action_attribution_windows": _ATTR_WINDOWS,
    })

async def _get_daily_insights(account_id: str, token: str, since: str, until: str) -> list:
    return await _meta_get_all(account_id, token, {
        "fields": _INS_FIELDS,
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1, "level": "account",
        "action_attribution_windows": _ATTR_WINDOWS,
    })

def _row_metrics(row: dict) -> dict:
    spend = float(row.get("spend", 0))
    imp   = int(row.get("impressions", 0))
    clk   = int(row.get("clicks", 0))
    reach = int(row.get("reach", 0))
    acts  = row.get("actions", [])
    vpa   = row.get("video_play_actions", [])
    link_clicks        = _extract_action(acts, ("link_click",))
    landing_page_views = _extract_action(acts, ("landing_page_view",))
    leads_form  = _extract_action(acts, _LEAD_FORM_TYPES)
    leads_web   = _extract_action(acts, _LEAD_WEB_TYPES)
    leads_sched  = _extract_action(acts, _cv_sched_types + ("schedule",))
    leads        = leads_form + leads_web
    purch = _extract_action(acts, _PURCHASE_TYPES)
    video_views = _extract_action(vpa, ("video_view",))
    thruplays   = _extract_action(acts, ("video_thruplay_watched",))
    return {
        "spend": spend, "impressions": imp, "clicks": clk, "reach": reach,
        "link_clicks": link_clicks, "landing_page_views": landing_page_views,
        "leads": leads, "leads_form": leads_form, "leads_web": leads_web, "leads_schedule": leads_sched,
        "purchases": purch, "video_views": video_views, "thruplays": thruplays,
        "ctr":  link_clicks / imp * 100 if imp else 0,
        "cpm":  spend / imp * 1000 if imp else 0,
        "cpc":  spend / link_clicks if link_clicks else 0,
        "cpl":  spend / leads if leads else None,
        "connect_rate":      landing_page_views / link_clicks * 100 if link_clicks else 0,
        "cost_per_schedule": spend / leads_sched if leads_sched else None,
    }

def _merge_totals(rows: list) -> dict:
    t = {"spend": 0.0, "impressions": 0, "clicks": 0, "reach": 0,
         "link_clicks": 0, "landing_page_views": 0,
         "leads": 0, "leads_form": 0, "leads_web": 0, "leads_schedule": 0,
         "purchases": 0, "video_views": 0, "thruplays": 0}
    for r in rows:
        for k in t: t[k] += r.get(k, 0)
    sp, im, lc, ld, ls, lpv = t["spend"], t["impressions"], t["link_clicks"], t["leads"], t["leads_schedule"], t["landing_page_views"]
    return {
        **t,
        "ctr": round(lc / im * 100 if im else 0, 2),
        "cpm": round(sp / im * 1000 if im else 0, 2),
        "cpc": round(sp / lc if lc else 0, 2),
        "cpl": round(sp / ld if ld else 0, 2) or None,
        "connect_rate":      round(lpv / lc * 100 if lc else 0, 2),
        "cost_per_schedule": round(sp / ls if ls else 0, 2) or None,
        "spend": round(sp, 2),
    }

def _pct(curr, prev):
    if not prev or prev == 0 or curr is None: return None
    return round((curr - prev) / abs(prev) * 100, 1)

async def _fetch_account_name(account_id: str, token: str) -> str:
    async with httpx.AsyncClient() as c:
        try:
            r = await c.get(f"{META_GRAPH_BASE}/{account_id}",
                params={"fields": "name", "access_token": token}, timeout=8)
            if r.status_code == 200:
                return r.json().get("name", account_id)
        except Exception:
            pass
    return account_id

async def _fetch_one_ad_creative(c: httpx.AsyncClient, ad_id: str, token: str) -> tuple:
    try:
        r = await c.get(f"{META_GRAPH_BASE}/{ad_id}", params={
            "fields": "effective_status,creative{thumbnail_url,image_url,instagram_permalink_url,effective_object_story_id,object_story_spec,link_url,object_url,asset_feed_spec{link_urls}}",
            "access_token": token,
        }, timeout=15)
        if r.status_code != 200:
            return ad_id, {}
        data = r.json()
        creative = data.get("creative", {})
        thumbnail = creative.get("image_url") or creative.get("thumbnail_url", "")
        link = creative.get("instagram_permalink_url", "")
        if not link:
            story_id = creative.get("effective_object_story_id", "")
            if story_id:
                link = f"https://www.facebook.com/{story_id}"
        oss = creative.get("object_story_spec", {})
        afs_urls = creative.get("asset_feed_spec", {}).get("link_urls", [])
        afs_lp = afs_urls[0].get("website_url", "") if afs_urls else ""
        lp = (
            creative.get("link_url") or
            creative.get("object_url") or
            oss.get("link_data", {}).get("link") or
            oss.get("video_data", {}).get("call_to_action", {}).get("value", {}).get("link") or
            oss.get("template_data", {}).get("link") or
            afs_lp or
            ""
        )
        return ad_id, {"thumbnail": thumbnail, "link": link, "lp": lp, "effective_status": data.get("effective_status", "UNKNOWN")}
    except Exception as e:
        print(f"[meta creative {ad_id}] {e}")
        return ad_id, {}

async def _load_custom_conversions():
    global _cv_sched_types, _cv_lead_web_types, _cv_loaded
    if _cv_loaded:
        return
    token = get_token()
    sched, lead_web = [], []
    _SCHED_KW = ("schedule", "agendamento", "booking", "booked", "reuniao", "reunião", "meeting", "appointment")
    _LEAD_KW  = ("lead", "formulario", "formulário", "jf_subs", "submit")
    seen = set()
    any_loaded = False
    async with httpx.AsyncClient() as c:
        for acc in get_accounts():
            try:
                r = await c.get(f"{META_GRAPH_BASE}/{acc}/customconversions", params={
                    "fields": "id,name,custom_event_type", "access_token": token, "limit": 200,
                }, timeout=15)
                if r.status_code == 200:
                    any_loaded = True
                    for cv in r.json().get("data", []):
                        at = f"offsite_conversion.custom.{cv['id']}"
                        if at in seen:
                            continue
                        seen.add(at)
                        nl = cv["name"].lower()
                        et = cv.get("custom_event_type", "")
                        if et == "SCHEDULE" or any(k in nl for k in _SCHED_KW):
                            sched.append(at)
                            print(f"[cv] schedule → {cv['name']} ({at})")
                        elif et == "LEAD" or any(k in nl for k in _LEAD_KW):
                            lead_web.append(at)
                            print(f"[cv] lead_web → {cv['name']} ({at})")
                        else:
                            print(f"[cv] ignored  → {cv['name']} ({at})")
                else:
                    print(f"[meta cv] {acc} returned {r.status_code}: {r.text[:200]}")
            except Exception as e:
                print(f"[meta cv] {e}")
    _cv_sched_types = tuple(sched)
    _cv_lead_web_types = tuple(lead_web)
    _cv_loaded = any_loaded

async def _fetch_campaign_data(account_id: str, token: str, level: str) -> dict:
    endpoint = "campaigns" if level == "campaign" else "adsets"
    fields = ("id,name,daily_budget,lifetime_budget,effective_status,optimization_goal,campaign_id"
              if level == "adset" else "id,name,daily_budget,lifetime_budget,effective_status")
    async with httpx.AsyncClient() as c:
        try:
            r = await c.get(f"{META_GRAPH_BASE}/{account_id}/{endpoint}", params={
                "fields": fields,
                "access_token": token,
                "limit": 500,
            }, timeout=15)
            if r.status_code == 200:
                return {item["name"]: item for item in r.json().get("data", [])}
            print(f"[meta {endpoint}] {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[meta {endpoint}] {e}")
    return {}

async def _fetch_ad_creatives_by_ids(ad_ids: list, token: str) -> dict:
    result = {}
    async with httpx.AsyncClient() as c:
        for i in range(0, len(ad_ids), 20):
            batch = ad_ids[i:i+20]
            tasks = [_fetch_one_ad_creative(c, ad_id, token) for ad_id in batch]
            for ad_id, info in await asyncio.gather(*tasks):
                if info:
                    result[ad_id] = info
    return result




# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI()
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    token = get_token()
    accounts = []
    for acc_id in get_accounts():
        name = await _fetch_account_name(acc_id, token) if token else acc_id
        accounts.append({"id": acc_id, "name": name})
    return templates.TemplateResponse("index.html", {
        "request": request,
        "accounts_json": json.dumps(accounts),
        "configured": bool(token and get_accounts()),
    })


@app.get("/api/metrics")
async def api_metrics(
    preset: str = "last_7d",
    since: str = None,
    until: str = None,
    account_id: str = None,
    level: str = "campaign",
):
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    if not token or not accounts_list:
        raise HTTPException(status_code=503, detail="META_ACCESS_TOKEN ou META_ACCOUNT_IDS não configurados no .env")

    d_since, d_until, p_since, p_until = _compute_period(
        preset if not (since and until) else None, since, until
    )
    accounts = ([account_id] if account_id and account_id in accounts_list else accounts_list)
    name_key = "campaign_name" if level == "campaign" else "adset_name"

    hidden        = get_hidden_campaigns()
    hidden_lower  = {h.lower() for h in hidden}
    merged, prev_merged, campaign_data = {}, {}, {}

    def _accumulate(target: dict, name: str, m: dict):
        if name in target:
            for k in ("spend","impressions","clicks","reach","link_clicks","landing_page_views","leads","leads_form","leads_web","leads_schedule","purchases","video_views","thruplays"):
                target[name][k] += m[k]
        else:
            target[name] = {**m, "name": name}

    for acc in accounts:
        for row in await _get_insights(acc, token, d_since, d_until, level):
            name = row.get(name_key, "")
            camp = row.get("campaign_name", name) if level == "adset" else name
            if name.strip().lower() in hidden_lower or camp.strip().lower() in hidden_lower:
                continue
            m = _row_metrics(row)
            _accumulate(merged, name, m)
            if level == "adset":
                merged[name]["campaign_name"] = camp
        for row in await _get_insights(acc, token, p_since, p_until, level):
            name = row.get(name_key, "")
            camp = row.get("campaign_name", name) if level == "adset" else name
            if name in hidden or camp in hidden:
                continue
            _accumulate(prev_merged, name, _row_metrics(row))
            if level == "adset":
                prev_merged[name]["campaign_name"] = camp
        if level in ("campaign", "adset"):
            cdata = await _fetch_campaign_data(acc, token, level)
            campaign_data.update(cdata)

    summary_curr = _merge_totals(list(merged.values()))
    summary_prev = _merge_totals(list(prev_merged.values()))
    summary = {k: v for k, v in summary_curr.items()}
    for k in ("spend","impressions","link_clicks","landing_page_views","reach","leads","leads_web","leads_form","leads_schedule","ctr","cpm","cpc","cpl","connect_rate","cost_per_schedule"):
        summary[f"{k}_delta"] = _pct(summary_curr.get(k), summary_prev.get(k))

    avg_cpl = summary_curr.get("cpl")
    avg_cps = summary_curr.get("cost_per_schedule")
    items = []
    for row in merged.values():
        sp, im, lc, ld, ls = row["spend"], row["impressions"], row["link_clicks"], row["leads"], row["leads_schedule"]
        lpv = row["landing_page_views"]
        row["ctr"]   = round(lc / im * 100 if im else 0, 2)
        row["cpm"]   = round(sp / im * 1000 if im else 0, 2)
        row["cpc"]   = round(sp / lc if lc else 0, 2)
        row["cpl"]   = round(sp / ld if ld else 0, 2) or None
        row["spend"] = round(sp, 2)
        row["connect_rate"]      = round(lpv / lc * 100 if lc else 0, 2)
        row["cost_per_schedule"] = round(sp / ls if ls else 0, 2) or None
        risks = []
        if row["ctr"] < 0.5 and im > 1000:        risks.append("Low CTR")
        if row["cpl"] and avg_cpl and row["cpl"] > avg_cpl * 2: risks.append("High CPL")
        if row["cost_per_schedule"] and avg_cps and row["cost_per_schedule"] > avg_cps * 2 and ls > 0: risks.append("High Cost/Appt")
        if sp > 30 and ld == 0 and im > 500:       risks.append("No conversions")
        row["risks"] = risks
        cinfo = campaign_data.get(row["name"], {})
        daily_b = cinfo.get("daily_budget")
        lifetime_b = cinfo.get("lifetime_budget")
        budget_cents = daily_b or lifetime_b
        row["budget"] = round(float(budget_cents) / 100, 2) if budget_cents else None
        row["budget_type"] = "daily" if daily_b else ("lifetime" if lifetime_b else None)
        row["status"] = cinfo.get("effective_status", "UNKNOWN")
        items.append(row)
    items.sort(key=lambda x: x["spend"], reverse=True)

    return JSONResponse({
        "since": d_since, "until": d_until,
        "prev_since": p_since, "prev_until": p_until,
        "accounts": accounts, "level": level,
        "summary": summary, "items": items,
    })


@app.get("/api/daily")
async def api_daily(
    preset: str = "last_7d",
    since: str = None,
    until: str = None,
    account_id: str = None,
):
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    if not token or not accounts_list:
        raise HTTPException(status_code=503, detail="META_ACCESS_TOKEN ou META_ACCOUNT_IDS não configurados no .env")

    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    accounts = ([account_id] if account_id and account_id in accounts_list else accounts_list)

    hidden_lower = {h.lower() for h in get_hidden_campaigns()}

    by_date: dict = {}
    for acc in accounts:
        rows = await _meta_get_all(acc, token, {
            "fields": f"campaign_name,{_INS_FIELDS}",
            "time_range": json.dumps({"since": d_since, "until": d_until}),
            "time_increment": 1, "level": "campaign",
            "action_attribution_windows": _ATTR_WINDOWS,
        })
        for row in rows:
            camp = (row.get("campaign_name") or "").strip().lower()
            if camp in hidden_lower:
                continue
            day = row.get("date_start", "")
            if day not in by_date:
                by_date[day] = {"date": day, "spend": 0.0, "impressions": 0, "clicks": 0,
                                "link_clicks": 0, "landing_page_views": 0, "leads": 0, "leads_schedule": 0}
            acts = row.get("actions", [])
            ls = _extract_action(acts, _cv_sched_types + ("schedule",))
            by_date[day]["spend"]               += float(row.get("spend", 0))
            by_date[day]["impressions"]         += int(row.get("impressions", 0))
            by_date[day]["clicks"]              += int(row.get("clicks", 0))
            by_date[day]["link_clicks"]         += _extract_action(acts, ("link_click",))
            by_date[day]["landing_page_views"]  += _extract_action(acts, ("landing_page_view",))
            by_date[day]["leads"]               += _extract_action(acts, _LEAD_FORM_TYPES + _LEAD_WEB_TYPES + _cv_lead_web_types + _cv_sched_types + ("schedule",))
            by_date[day]["leads_schedule"]      += ls

    days = sorted(by_date.values(), key=lambda x: x["date"])
    for d in days:
        sp = d["spend"]; im = d["impressions"]; lc = d["link_clicks"]
        ld = d["leads"]; ls = d["leads_schedule"]; lpv = d["landing_page_views"]
        d["spend"]         = round(sp, 2)
        d["ctr"]           = round(lc / im * 100 if im else 0, 2)
        d["cpm"]           = round(sp / im * 1000 if im else 0, 2)
        d["cpc"]           = round(sp / lc if lc else 0, 2)
        d["cpl"]           = round(sp / ld if ld else 0, 2) if ld else None
        d["connect_rate"]  = round(lpv / lc * 100 if lc else 0, 2)
        d["conv_rate"]     = round(ld / lpv * 100 if lpv else 0, 2)
        d["cost_per_schedule"] = round(sp / ls if ls else 0, 2) if ls else None
    return JSONResponse({"days": days})


@app.get("/api/creatives")
async def api_creatives(
    preset: str = "last_7d",
    since: str = None,
    until: str = None,
    account_id: str = None,
):
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    if not token or not accounts_list:
        raise HTTPException(status_code=503, detail="META_ACCESS_TOKEN ou META_ACCOUNT_IDS não configurados no .env")

    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    accounts = ([account_id] if account_id and account_id in accounts_list else accounts_list)

    all_ads = []
    for acc in accounts:
        rows = await _meta_get_all(acc, token, {
            "fields": f"ad_id,ad_name,campaign_id,campaign_name,{_INS_FIELDS}",
            "time_range": json.dumps({"since": d_since, "until": d_until}),
            "level": "ad",
            "action_attribution_windows": _ATTR_WINDOWS,
        })
        hidden_c = get_hidden_campaigns()
        hidden_c_lower = {h.lower() for h in hidden_c}
        before = len(rows)
        rows = [r for r in rows if (r.get("campaign_name") or "").strip().lower() not in hidden_c_lower]
        print(f"[creatives] acc={acc} total_rows={before} after_filter={len(rows)} hidden={hidden_c_lower}")
        ad_ids = [r.get("ad_id", "") for r in rows if r.get("ad_id")]
        creatives = await _fetch_ad_creatives_by_ids(ad_ids, token)
        for row in rows:
            ad_id = row.get("ad_id", "")
            m = _row_metrics(row)
            cinfo = creatives.get(ad_id, {})
            sp = m["spend"]; ld = m["leads"]; im = m["impressions"]
            lc = m["link_clicks"]; lpv = m["landing_page_views"]
            vv = m["video_views"]
            ls = m["leads_schedule"]
            all_ads.append({
                "id": ad_id,
                "name": row.get("ad_name", ""),
                "campaign_id": row.get("campaign_id", ""),
                "campaign_name": row.get("campaign_name", ""),
                "thumbnail": cinfo.get("thumbnail", ""),
                "link": cinfo.get("link", ""),
                "lp": cinfo.get("lp", ""),
                "effective_status": cinfo.get("effective_status", "UNKNOWN"),
                "spend": round(sp, 2),
                "impressions": im, "clicks": m["clicks"], "reach": m["reach"],
                "link_clicks": lc, "landing_page_views": lpv,
                "leads": ld, "leads_form": m["leads_form"], "leads_web": m["leads_web"], "leads_schedule": ls,
                "video_views": vv, "thruplays": m["thruplays"],
                "ctr":  round(lc / im * 100 if im else 0, 2),
                "cpm":  round(sp / im * 1000 if im else 0, 2),
                "cpc":  round(sp / lc if lc else 0, 2),
                "cpl":  round(sp / ld if ld else 0, 2) or None,
                "connect_rate":      round(lpv / lc * 100 if lc else 0, 2),
                "cost_per_schedule": round(sp / ls if ls else 0, 2) or None,
                "hook_rate": round(vv / im * 100 if im and vv else 0, 2),
            })

    all_ads.sort(key=lambda x: x["spend"], reverse=True)
    return JSONResponse({"ads": all_ads, "since": d_since, "until": d_until})


@app.get("/api/lp")
async def api_lp(
    preset: str = "last_7d",
    since: str = None,
    until: str = None,
    account_id: str = None,
):
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    if not token or not accounts_list:
        raise HTTPException(status_code=503, detail="META_ACCESS_TOKEN ou META_ACCOUNT_IDS não configurados no .env")

    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    accounts = ([account_id] if account_id and account_id in accounts_list else accounts_list)

    lp_map: dict = {}
    hidden_c = get_hidden_campaigns()
    hidden_c_lower = {h.lower() for h in hidden_c}

    for acc in accounts:
        rows = await _meta_get_all(acc, token, {
            "fields": f"ad_id,ad_name,campaign_id,campaign_name,{_INS_FIELDS}",
            "time_range": json.dumps({"since": d_since, "until": d_until}),
            "level": "ad",
            "action_attribution_windows": _ATTR_WINDOWS,
        })
        rows = [r for r in rows if (r.get("campaign_name") or "").strip().lower() not in hidden_c_lower]
        ad_ids = [r.get("ad_id", "") for r in rows if r.get("ad_id")]
        creatives = await _fetch_ad_creatives_by_ids(ad_ids, token)
        for aid, info in creatives.items():
            _ad_lp_cache[aid] = info.get("lp") or ""

        for row in rows:
            ad_id = row.get("ad_id", "")
            m = _row_metrics(row)
            cinfo = creatives.get(ad_id, {})
            lp = cinfo.get("lp") or "No LP"

            ls = m["leads_schedule"]

            if lp not in lp_map:
                lp_map[lp] = {"lp": lp, "spend": 0.0, "impressions": 0, "clicks": 0,
                               "link_clicks": 0, "landing_page_views": 0, "reach": 0,
                               "leads": 0, "leads_form": 0, "leads_web": 0, "leads_schedule": 0,
                               "video_views": 0, "thruplays": 0, "ad_count": 0}
            e = lp_map[lp]
            for k in ("impressions","clicks","link_clicks","landing_page_views","reach","leads","leads_form","leads_web","video_views","thruplays"):
                e[k] += m[k]
            e["spend"]          += m["spend"]
            e["leads_schedule"] += ls
            e["ad_count"]       += 1

    result = []
    for e in lp_map.values():
        sp = e["spend"]; im = e["impressions"]; lc = e["link_clicks"]
        ld = e["leads"]; ls2 = e["leads_schedule"]; lpv = e["landing_page_views"]
        e["spend"]             = round(sp, 2)
        e["ctr"]               = round(lc / im * 100 if im else 0, 2)
        e["cpm"]               = round(sp / im * 1000 if im else 0, 2)
        e["cpc"]               = round(sp / lc if lc else 0, 2) or None
        e["cpl"]               = round(sp / ld if ld else 0, 2) or None
        e["connect_rate"]      = round(lpv / lc * 100 if lc else 0, 2)
        e["cost_per_schedule"] = round(sp / ls2 if ls2 else 0, 2) or None
        e["conv_rate"]         = round(ld / lpv * 100 if lpv else 0, 2)
        result.append(e)

    result.sort(key=lambda x: x["spend"], reverse=True)
    return JSONResponse({"items": result, "since": d_since, "until": d_until})


@app.get("/api/lp_daily")
async def api_lp_daily(
    preset: str = "last_7d",
    since: str = None,
    until: str = None,
    account_id: str = None,
    q: str = "",
):
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    if not token or not accounts_list:
        raise HTTPException(status_code=503, detail="META_ACCESS_TOKEN ou META_ACCOUNT_IDS não configurados no .env")

    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    accounts = ([account_id] if account_id and account_id in accounts_list else accounts_list)
    hidden_c_lower = {h.lower() for h in get_hidden_campaigns()}
    q_lower = q.strip().lower()

    by_date: dict = {}
    for acc in accounts:
        rows = await _meta_get_all(acc, token, {
            "fields": f"ad_id,campaign_name,date_start,{_INS_FIELDS}",
            "time_range": json.dumps({"since": d_since, "until": d_until}),
            "level": "ad",
            "time_increment": 1,
            "action_attribution_windows": _ATTR_WINDOWS,
        })
        rows = [r for r in rows if (r.get("campaign_name") or "").strip().lower() not in hidden_c_lower]

        # Populate LP cache for any new ad_ids
        unknown_ids = [r["ad_id"] for r in rows if r.get("ad_id") and r.get("ad_id") not in _ad_lp_cache]
        if unknown_ids:
            new_creatives = await _fetch_ad_creatives_by_ids(list(set(unknown_ids)), token)
            for aid, info in new_creatives.items():
                _ad_lp_cache[aid] = info.get("lp") or ""

        for row in rows:
            ad_id = row.get("ad_id", "")
            lp = _ad_lp_cache.get(ad_id, "")
            if q_lower and q_lower not in lp.lower():
                continue
            day = row.get("date_start", "")
            if day not in by_date:
                by_date[day] = {"date": day, "spend": 0.0, "impressions": 0, "clicks": 0,
                                "link_clicks": 0, "landing_page_views": 0, "leads": 0, "leads_schedule": 0}
            m = _row_metrics(row)
            ls = m["leads_schedule"]
            d = by_date[day]
            d["spend"]              += m["spend"]
            d["impressions"]        += m["impressions"]
            d["clicks"]             += m["clicks"]
            d["link_clicks"]        += m["link_clicks"]
            d["landing_page_views"] += m["landing_page_views"]
            d["leads"]              += m["leads"]
            d["leads_schedule"]     += ls

    days = sorted(by_date.values(), key=lambda x: x["date"])
    for d in days:
        sp = d["spend"]; im = d["impressions"]; lc = d["link_clicks"]
        ld = d["leads"]; ls = d["leads_schedule"]; lpv = d["landing_page_views"]
        d["spend"]         = round(sp, 2)
        d["ctr"]           = round(lc / im * 100 if im else 0, 2)
        d["cpm"]           = round(sp / im * 1000 if im else 0, 2)
        d["cpc"]           = round(sp / lc if lc else 0, 2)
        d["cpl"]           = round(sp / ld if ld else 0, 2) if ld else None
        d["connect_rate"]  = round(lpv / lc * 100 if lc else 0, 2)
        d["conv_rate"]     = round(ld / lpv * 100 if lpv else 0, 2)
        d["cost_per_schedule"] = round(sp / ls if ls else 0, 2) if ls else None
    return JSONResponse({"days": days, "since": d_since, "until": d_until, "q": q})


@app.get("/api/debug/actions")
async def api_debug_actions(
    preset: str = "last_7d",
    since: str = None,
    until: str = None,
    account_id: str = None,
):
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    accounts = ([account_id] if account_id and account_id in accounts_list else accounts_list)

    action_totals: dict = {}
    by_campaign: dict = {}
    async with httpx.AsyncClient() as c:
        for acc in accounts:
            rows = await _meta_get_all(acc, token, {
                "fields": "campaign_name,actions",
                "time_range": json.dumps({"since": d_since, "until": d_until}),
                "level": "campaign",
            })
            for row in rows:
                cname = row.get("campaign_name", "unknown")
                by_campaign.setdefault(cname, {})
                for act in row.get("actions", []):
                    t = act.get("action_type", "")
                    v = int(float(act.get("value", 0)))
                    action_totals[t] = action_totals.get(t, 0) + v
                    by_campaign[cname][t] = by_campaign[cname].get(t, 0) + v

    return JSONResponse({
        "period": f"{d_since} → {d_until}",
        "cv_schedule_types": list(_cv_sched_types),
        "action_types": {k: v for k, v in sorted(action_totals.items(), key=lambda x: -x[1])},
        "by_campaign": {
            cname: {k: v for k, v in sorted(acts.items(), key=lambda x: -x[1])}
            for cname, acts in sorted(by_campaign.items())
        },
    })


@app.get("/api/debug/conversions")
async def api_debug_conversions():
    await _load_custom_conversions()
    token = get_token()
    result = {}
    async with httpx.AsyncClient() as c:
        for acc in get_accounts():
            try:
                r = await c.get(f"{META_GRAPH_BASE}/{acc}/customconversions", params={
                    "fields": "id,name,custom_event_type", "access_token": token, "limit": 200,
                }, timeout=15)
                if r.status_code == 200:
                    for cv in r.json().get("data", []):
                        result[f"offsite_conversion.custom.{cv['id']}"] = {
                            "name": cv.get("name"),
                            "event_type": cv.get("custom_event_type"),
                        }
            except Exception as e:
                result[f"error_{acc}"] = str(e)
    return JSONResponse({
        "custom_conversions": result,
        "cv_schedule_mapped": list(_cv_sched_types),
        "cv_lead_web_mapped": list(_cv_lead_web_types),
    })


# ── Executive Summary constants ───────────────────────────────────────────────

# Canonical MKT attributions (source of truth = HubSpot Attribution field)
MKT_ATTRIBUTIONS = {
    "Meta Ads - On-site Conversion",
    "Meta Ads - On Site/Conversion",   # legacy variant
    "Meta Ads - Callingly",
    "Meta Ads - Callingly/Instant Forms",
    "Meta Ads",
    "Google Ads",
    "Google Search",
    "Google Search - Organic",
    "Facebook Ads",
    "Twitter Ads",
    "LinkedIn Ads",
    "Reddit Ads",
    "Direct Traffic",
    "Organic Social",
}

def _attr_channel(attr: str, utm_campaign: str = "") -> str:
    """Map raw attribution + utm_campaign to a display channel key.
    For Meta contacts, utm_campaign drives the sub-channel split:
      'Instant' in utm_campaign → meta_callingly (Instant Form)
      otherwise               → meta_onsite (On-Site/Conversion)
    """
    a = attr.lower()
    if "meta" in a or "facebook" in a:
        if "instant" in utm_campaign.lower():
            return "meta_callingly"
        return "meta_onsite"
    if "google ads" in a:                 return "google_paid"
    if "google" in a:                     return "google_organic"
    if "twitter" in a or "x ads" in a:   return "x"
    if "linkedin" in a:                   return "linkedin"
    if "reddit" in a:                     return "reddit"
    if "direct" in a:                     return "direct"
    if "organic" in a:                    return "organic"
    return "other"

# ── HubSpot live cache (auto-refresh every 15 min) ───────────────────────────
_HS_APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxVmtRyJiFex9yQMPDsk6ivMY3f6clHHe0mdEBnxMxiP02yWoaBzGTmv5-8yxopexOs/exec"
_HS_MB_DATE_COL    = "Date entered \"Meeting Scheduled (Placements — Inbound Sales Stage)\""
_HS_EL_SENT_COL    = "Date Engagement Letter Was Sent"
_HS_EL_SIGNED_COL  = "Date entered \"Closed Won (Placements — Inbound Sales Stage)\""
_HS_CACHE_TTL      = 900  # seconds (15 min)
_HS_PLACEHOLDERS   = {"{{campaign.name}}", "{{ad.name}}", ""}

# Three separate caches — one per sheet tab
_hs_mem:    dict = {"contacts": [], "loaded_at": 0.0}   # MB tab
_hs_nl_mem: dict = {"contacts": [], "loaded_at": 0.0}   # New Leads tab
_hs_mh_mem: dict = {"contacts": [], "loaded_at": 0.0}   # MH tab

def _normalize_meta_attr(attr: str, utm_campaign: str) -> str:
    """Split generic 'Meta Ads' (and Facebook variants) into Callingly vs On-site/Conversion."""
    a = attr.lower()
    if "meta" in a or "facebook" in a:
        if "instant" in utm_campaign.lower():
            return "Meta Ads - Callingly/Instant Forms"
        return "Meta Ads - On-site Conversion"
    return attr

def _clean_utm(val: str) -> str:
    if not val:
        return ""
    try:
        val = urllib.parse.unquote_plus(val)
    except Exception:
        pass
    val = val.strip()
    return "" if val in _HS_PLACEHOLDERS else val

def _parse_hs_date(val: str) -> str:
    """Convert any HubSpot date string to YYYY-MM-DD. Returns '' if unparseable."""
    import datetime as _dt
    val = str(val or "").strip()
    if not val:
        return ""
    if len(val) >= 10 and val[4] == "-" and val[7] == "-":
        return val[:10]
    try:
        return _dt.datetime.strptime(val[:24], "%a %b %d %Y %H:%M:%S").strftime("%Y-%m-%d")
    except Exception:
        pass
    return val[:10] if len(val) >= 10 else ""

def _truthy(v) -> bool:
    s = str(v or "").strip()
    return bool(s) and s != "(No value)"

def _hs_url(sheet: str) -> str:
    return f"{_HS_APPS_SCRIPT_URL}?sheet={sheet}"

def _fetch_hs_contacts() -> list:
    """Fetch MB tab — one row per Meeting Booked contact."""
    import httpx as _httpx
    rows = _httpx.get(_hs_url("mb"), follow_redirects=True, timeout=30).json()
    contacts = []
    for row in rows:
        date = _parse_hs_date(str(row.get(_HS_MB_DATE_COL, "") or "").strip()
                               or str(row.get("Create Date", "") or "").strip())
        if not date or len(date) < 10:
            continue
        mql_val     = str(row.get("MQL", "") or "").strip()
        sql_val     = str(row.get("SQL", "") or "").strip()
        utm_campaign = _clean_utm(str(row.get("utm_campaign", "") or "")) or "(no utm_campaign)"
        raw_attr    = str(row.get("Attribution (Contact-Level)", "") or "").strip() or "(unknown)"
        attribution = _normalize_meta_attr(raw_attr, utm_campaign)
        contacts.append({
            "date":         date,
            "email":        str(row.get("Email", "") or "").strip().lower(),
            "mql":          mql_val == "Yes",
            "sql":          sql_val == "Yes",
            "el_sent":      _truthy(row.get(_HS_EL_SENT_COL, "")),
            "el_signed":    _truthy(row.get(_HS_EL_SIGNED_COL, "")),
            "attribution":  attribution,
            "utm_source":   _clean_utm(str(row.get("utm_source",  "") or "")) or "",
            "utm_campaign": utm_campaign,
            "utm_medium":   _clean_utm(str(row.get("utm_medium",  "") or "")) or "",
            "utm_content":  _clean_utm(str(row.get("utm_content", "") or "")) or "",
        })
    contacts.sort(key=lambda x: x["date"])
    return contacts

def _fetch_hs_new_leads() -> list:
    """Fetch New Leads tab — all leads by Create Date."""
    import httpx as _httpx
    rows = _httpx.get(_hs_url("new_leads"), follow_redirects=True, timeout=30).json()
    contacts = []
    for row in rows:
        date = _parse_hs_date(str(row.get("Create Date", "") or ""))
        if not date or len(date) < 10:
            continue
        utm_campaign = _clean_utm(str(row.get("utm_campaign", "") or "")) or "(no utm_campaign)"
        raw_attr    = str(row.get("Attribution (Contact-Level)", "") or "").strip() or "(unknown)"
        attribution = _normalize_meta_attr(raw_attr, utm_campaign)
        contacts.append({
            "date":         date,
            "email":        str(row.get("Email", "") or "").strip().lower(),
            "attribution":  attribution,
            "utm_source":   _clean_utm(str(row.get("utm_source",  "") or "")) or "",
            "utm_campaign": utm_campaign,
            "utm_medium":   _clean_utm(str(row.get("utm_medium",  "") or "")) or "",
            "utm_content":  _clean_utm(str(row.get("utm_content", "") or "")) or "",
        })
    contacts.sort(key=lambda x: x["date"])
    return contacts

def _fetch_hs_mh() -> list:
    """Fetch MH tab — one row per Meeting Held, dated by Meeting start time."""
    import httpx as _httpx
    rows = _httpx.get(_hs_url("mh"), follow_redirects=True, timeout=30).json()
    contacts = []
    for row in rows:
        date = _parse_hs_date(str(row.get("Meeting start time", "") or ""))
        if not date or len(date) < 10:
            continue
        outcome = str(row.get("Meeting outcome", "") or "").strip()
        if outcome in {"No Show", "(No value)"}:
            continue
        utm_campaign = _clean_utm(str(row.get("utm_campaign", "") or "")) or "(no utm_campaign)"
        raw_attr    = str(row.get("Attribution (Contact-Level)", "") or "").strip() or "(unknown)"
        attribution = _normalize_meta_attr(raw_attr, utm_campaign)
        contacts.append({
            "date":        date,
            "email":       str(row.get("Email", "") or "").strip().lower(),
            "attribution":  attribution,
            "utm_campaign": utm_campaign,
            "utm_medium":   _clean_utm(str(row.get("utm_medium",  "") or "")) or "",
        })
    contacts.sort(key=lambda x: x["date"])
    return contacts

def _get_hs_contacts() -> list:
    now = time.time()
    if now - _hs_mem["loaded_at"] > _HS_CACHE_TTL:
        try:
            _hs_mem["contacts"]  = _fetch_hs_contacts()
            _hs_mem["loaded_at"] = now
        except Exception as e:
            print(f"[HS MB] fetch failed: {e}")
            if not _hs_mem["contacts"]:
                _hs_cache_path = os.path.join(os.path.dirname(__file__), "hubspot_cache.json")
                if os.path.exists(_hs_cache_path):
                    with open(_hs_cache_path, "r", encoding="utf-8") as f:
                        _hs_mem["contacts"] = json.load(f).get("contacts", [])
    return _hs_mem["contacts"]

def _get_hs_new_leads() -> list:
    now = time.time()
    if now - _hs_nl_mem["loaded_at"] > _HS_CACHE_TTL:
        try:
            _hs_nl_mem["contacts"]  = _fetch_hs_new_leads()
            _hs_nl_mem["loaded_at"] = now
        except Exception as e:
            print(f"[HS NL] fetch failed: {e}")
    return _hs_nl_mem["contacts"]

def _get_hs_mh() -> list:
    now = time.time()
    if now - _hs_mh_mem["loaded_at"] > _HS_CACHE_TTL:
        try:
            _hs_mh_mem["contacts"]  = _fetch_hs_mh()
            _hs_mh_mem["loaded_at"] = now
        except Exception as e:
            print(f"[HS MH] fetch failed: {e}")
    return _hs_mh_mem["contacts"]

def _load_hs_contacts(d_since: str, d_until: str, mkt_only: bool = True):
    """Return MB contacts filtered by date range and optionally MKT attribution."""
    all_c = _get_hs_contacts()
    result = [c for c in all_c if d_since <= c["date"] <= d_until]
    if mkt_only:
        result = [c for c in result if c.get("attribution", "") in MKT_ATTRIBUTIONS]
    return result

def _load_hs_new_leads(d_since: str, d_until: str, mkt_only: bool = True):
    """Return New Leads filtered by date range and optionally MKT attribution."""
    all_c = _get_hs_new_leads()
    result = [c for c in all_c if d_since <= c["date"] <= d_until]
    if mkt_only:
        result = [c for c in result if c.get("attribution", "") in MKT_ATTRIBUTIONS]
    return result

def _load_hs_mh(d_since: str, d_until: str, mkt_only: bool = True):
    """Return MH contacts filtered by date range and optionally MKT attribution."""
    all_c = _get_hs_mh()
    result = [c for c in all_c if d_since <= c["date"] <= d_until]
    if mkt_only:
        result = [c for c in result if c.get("attribution", "") in MKT_ATTRIBUTIONS]
    return result


@app.get("/api/executive/funnel")
async def api_executive_funnel(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 1.1 — Funnel Overview (Total MKT). Returns stage counts + conv rates."""
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    nl_contacts = _load_hs_new_leads(d_since, d_until, mkt_only=True)
    mb_contacts = _load_hs_contacts(d_since, d_until, mkt_only=True)
    mh_contacts = _load_hs_mh(d_since, d_until, mkt_only=True)

    nl        = len(nl_contacts)
    mb        = len(mb_contacts)
    mh        = len(mh_contacts)
    mql       = sum(1 for c in mb_contacts if c.get("mql"))
    sql       = sum(1 for c in mb_contacts if c.get("sql"))
    el_sent   = sum(1 for c in mb_contacts if c.get("el_sent"))
    el_signed = sum(1 for c in mb_contacts if c.get("el_signed"))

    def _pct_of_mb(num):
        return round(num / mb * 100, 1) if mb else None

    def _pct_of_nl(num):
        return round(num / nl * 100, 1) if nl else None

    stages = [
        {"stage": "New Leads",           "count": nl,        "conv_from_mb": None},
        {"stage": "Meeting Booked (MB)", "count": mb,        "conv_from_mb": _pct_of_nl(mb)},
        {"stage": "Meeting Held (MH)",   "count": mh,        "conv_from_mb": _pct_of_mb(mh)},
        {"stage": "MQL",                 "count": mql,       "conv_from_mb": _pct_of_mb(mql)},
        {"stage": "SQL",                 "count": sql,       "conv_from_mb": _pct_of_mb(sql)},
        {"stage": "EL Sent",             "count": el_sent,   "conv_from_mb": _pct_of_mb(el_sent)},
        {"stage": "EL Signed",           "count": el_signed, "conv_from_mb": _pct_of_mb(el_signed)},
    ]

    return JSONResponse({
        "since": d_since, "until": d_until,
        "stages": stages,
        "totals": {"nl": nl, "mb": mb, "mh": mh, "mql": mql, "sql": sql,
                   "el_sent": el_sent, "el_signed": el_signed},
    })


@app.get("/api/executive/spend")
async def api_executive_spend(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 1.2 — Spend Summary by Channel. Combines Meta API spend + HubSpot funnel counts."""
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()

    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    # ── Meta spend from API — split by campaign (onsite vs callingly) ────────
    meta_spend = 0.0
    spend_onsite    = 0.0
    spend_callingly = 0.0
    if token and accounts_list:
        for acc in accounts_list:
            camp_rows = await _get_insights(acc, token, d_since, d_until, "campaign")
            for row in camp_rows:
                s = float(row.get("spend", 0))
                meta_spend += s
                name = row.get("campaign_name", "").lower()
                if "instant" in name:
                    spend_callingly += s
                else:
                    spend_onsite += s
    meta_spend       = round(meta_spend, 2)
    spend_onsite     = round(spend_onsite, 2)
    spend_callingly  = round(spend_callingly, 2)

    # ── HubSpot funnel by channel ─────────────────────────────────────────────
    contacts    = _load_hs_contacts(d_since, d_until, mkt_only=True)
    mh_contacts = _load_hs_mh(d_since, d_until, mkt_only=True)

    # Accumulators per channel key
    channels: dict = {}
    def _ensure(key):
        if key not in channels:
            channels[key] = {"mb": 0, "mql": 0, "mh": 0, "sql": 0, "el_sent": 0, "el_signed": 0}
        return channels[key]

    for c in contacts:
        ch = _attr_channel(c.get("attribution", ""), c.get("utm_campaign", ""))
        _ensure(ch)
        channels[ch]["mb"]  += 1
        if c.get("mql"):       channels[ch]["mql"]      += 1
        if c.get("sql"):       channels[ch]["sql"]      += 1
        if c.get("el_sent"):   channels[ch]["el_sent"]  += 1
        if c.get("el_signed"): channels[ch]["el_signed"]+= 1

    # MH from dedicated tab (dated by meeting start time)
    for c in mh_contacts:
        ch = _attr_channel(c.get("attribution", ""), c.get("utm_campaign", ""))
        _ensure(ch)
        channels[ch]["mh"] += 1

    def _cost(spend, count):
        return round(spend / count, 2) if count else None

    def _row(label, spend, ch_key, sub=False):
        d = channels.get(ch_key, {"mb": 0, "mql": 0, "mh": 0, "sql": 0, "el_sent": 0, "el_signed": 0})
        mb, mql, mh, sql = d["mb"], d["mql"], d["mh"], d["sql"]
        el_sent, el_signed = d["el_sent"], d["el_signed"]
        return {
            "channel": label,
            "sub": sub,
            "spend": spend,
            "mb":        mb,        "cpmb":           _cost(spend, mb)        if spend else None,
            "mql":       mql,       "cpmql":          _cost(spend, mql)       if spend else None,
            "mh":        mh,        "cpmh":           _cost(spend, mh)        if spend else None,
            "sql":       sql,       "cpsql":          _cost(spend, sql)       if spend else None,
            "el_sent":   el_sent,   "cost_el_sent":   _cost(spend, el_sent)   if spend else None,
            "el_signed": el_signed, "cost_el_signed": _cost(spend, el_signed) if spend else None,
        }

    # Total MKT funnel counts (same source as Panel 1.1)
    total_mb        = len(contacts)
    total_mql       = sum(1 for c in contacts if c.get("mql"))
    total_mh        = len(mh_contacts)
    total_sql       = sum(1 for c in contacts if c.get("sql"))
    total_el_sent   = sum(1 for c in contacts if c.get("el_sent"))
    total_el_signed = sum(1 for c in contacts if c.get("el_signed"))

    # Meta-attributed counts for the Meta Ads row
    _zero = {"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
    meta_onsite    = channels.get("meta_onsite",    _zero)
    meta_callingly = channels.get("meta_callingly", _zero)
    meta_mb        = meta_onsite["mb"]        + meta_callingly["mb"]
    meta_mql       = meta_onsite["mql"]       + meta_callingly["mql"]
    meta_mh        = meta_onsite["mh"]        + meta_callingly["mh"]
    meta_sql       = meta_onsite["sql"]       + meta_callingly["sql"]
    meta_el_sent   = meta_onsite["el_sent"]   + meta_callingly["el_sent"]
    meta_el_signed = meta_onsite["el_signed"] + meta_callingly["el_signed"]

    rows = [
        {
            "channel": "Meta Ads", "sub": False,
            "spend": meta_spend,
            "mb":        meta_mb,        "cpmb":           _cost(meta_spend, meta_mb),
            "mql":       meta_mql,       "cpmql":          _cost(meta_spend, meta_mql),
            "mh":        meta_mh,        "cpmh":           _cost(meta_spend, meta_mh),
            "sql":       meta_sql,       "cpsql":          _cost(meta_spend, meta_sql),
            "el_sent":   meta_el_sent,   "cost_el_sent":   _cost(meta_spend, meta_el_sent),
            "el_signed": meta_el_signed, "cost_el_signed": _cost(meta_spend, meta_el_signed),
        },
        _row(">> On-Site Conversion",        spend_onsite,    "meta_onsite",    sub=True),
        _row(">> Instant Form (Callingly)", spend_callingly, "meta_callingly", sub=True),
        _row("Google Ads",                 None, "google_paid"),
        _row("Google Search - Organic",    None, "google_organic"),
        _row("Organic Social",             None, "organic"),
        _row("Direct Traffic",             None, "direct"),
        _row("YouTube",                    None, "youtube"),
        _row("X Ads",                      None, "x"),
        _row("LinkedIn",                   None, "linkedin"),
    ]

    rows.append({
        "channel": "Total (all MKT)", "sub": False, "total": True,
        "spend": meta_spend,
        "mb":      total_mb,      "cpmb":         _cost(meta_spend, total_mb),
        "mql":     total_mql,     "cpmql":        _cost(meta_spend, total_mql),
        "mh":      total_mh,      "cpmh":         _cost(meta_spend, total_mh),
        "sql":     total_sql,     "cpsql":        _cost(meta_spend, total_sql),
        "el_sent":   total_el_sent,   "cost_el_sent":   _cost(meta_spend, total_el_sent),
        "el_signed": total_el_signed, "cost_el_signed": _cost(meta_spend, total_el_signed),
    })

    return JSONResponse({
        "since": d_since, "until": d_until,
        "rows": rows,
        "meta_spend_total": meta_spend,
    })


@app.get("/api/executive/attribution")
async def api_executive_attribution(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 1.3 — New Leads per MKT Attribution. Groups New Leads tab by attribution field."""
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    contacts = _load_hs_new_leads(d_since, d_until, mkt_only=False)

    counts: dict = {}
    for c in contacts:
        attr = c.get("attribution", "(unknown)") or "(unknown)"
        counts[attr] = counts.get(attr, 0) + 1

    total = len(contacts)
    rows = sorted(
        [{"attribution": k, "count": v, "pct": round(v / total * 100, 1) if total else 0}
         for k, v in counts.items()],
        key=lambda x: -x["count"]
    )

    return JSONResponse({
        "since": d_since, "until": d_until,
        "total": total,
        "rows": rows,
    })


@app.get("/api/executive/attr_breakdown")
async def api_executive_attr_breakdown(preset: str = "this_month", since: str = None, until: str = None):
    """Panels 1.4-1.8 — Per-attribution breakdown for MB, MH, SQL, EL Sent, EL Signed."""
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    nl_all = _load_hs_new_leads(d_since, d_until, mkt_only=False)
    mb_all = _load_hs_contacts(d_since, d_until, mkt_only=False)
    mh_all = _load_hs_mh(d_since, d_until, mkt_only=False)

    agg: dict = {}
    def _get(attr):
        if attr not in agg:
            agg[attr] = {"nl": 0, "mb": 0, "mh": 0, "sql": 0, "el_sent": 0, "el_signed": 0}
        return agg[attr]

    for c in nl_all: _get(c["attribution"])["nl"] += 1
    for c in mb_all:
        d = _get(c["attribution"])
        d["mb"] += 1
        if c.get("sql"):      d["sql"]      += 1
        if c.get("el_sent"):  d["el_sent"]  += 1
        if c.get("el_signed"):d["el_signed"]+= 1
    for c in mh_all: _get(c["attribution"])["mh"] += 1

    total_nl       = sum(d["nl"]       for d in agg.values())
    total_mb       = sum(d["mb"]       for d in agg.values())
    total_mh       = sum(d["mh"]       for d in agg.values())
    total_sql      = sum(d["sql"]      for d in agg.values())
    total_el_sent  = sum(d["el_sent"]  for d in agg.values())
    total_el_signed= sum(d["el_signed"]for d in agg.values())

    # Meta spend for CPMB
    await _load_custom_conversions()
    token = get_token()
    accounts_list = get_accounts()
    spend_callingly = 0.0
    spend_onsite    = 0.0
    if token and accounts_list:
        try:
            for acc in accounts_list:
                camp_rows = await _get_insights(acc, token, d_since, d_until, "campaign")
                for row in camp_rows:
                    s = float(row.get("spend", 0))
                    if "instant" in row.get("campaign_name", "").lower():
                        spend_callingly += s
                    else:
                        spend_onsite += s
        except Exception:
            pass

    def _attr_spend(attr):
        a = attr.lower()
        if "callingly" in a or "instant" in a: return spend_callingly or None
        if "meta" in a or "facebook" in a:     return spend_onsite    or None
        return None

    def _cost(sp, n): return round(sp / n, 2) if sp and n else None
    def _pct(n, total): return round(n / total * 100, 1) if total else 0
    def _rate(n, d):    return round(n / d * 100, 1)     if d    else None

    rows = []
    for attr, d in agg.items():
        sp = _attr_spend(attr)
        rows.append({
            "attribution":   attr,
            "nl":            d["nl"],       "nl_pct":       _pct(d["nl"],       total_nl),
            "mb":            d["mb"],       "mb_pct":       _pct(d["mb"],       total_mb),
            "spend":         round(sp, 2) if sp else None,
            "cpmb":          _cost(sp, d["mb"]),
            "mh":            d["mh"],       "mh_pct":       _pct(d["mh"],       total_mh),
            "cpmh":          _cost(sp, d["mh"]),
            "show_up_rate":  _rate(d["mh"], d["mb"]),
            "sql":           d["sql"],      "sql_pct":      _pct(d["sql"],      total_sql),
            "cpsql":         _cost(sp, d["sql"]),
            "sql_rate":      _rate(d["sql"], d["nl"]),
            "el_sent":       d["el_sent"],  "el_sent_pct":  _pct(d["el_sent"],  total_el_sent),
            "el_signed":     d["el_signed"],"el_signed_pct":_pct(d["el_signed"],total_el_signed),
            "close_rate":    _rate(d["el_signed"], d["sql"]),
        })

    rows.sort(key=lambda x: -x["mb"])

    return JSONResponse({
        "since": d_since, "until": d_until,
        "rows": rows,
        "totals": {
            "nl": total_nl, "mb": total_mb, "mh": total_mh,
            "sql": total_sql, "el_sent": total_el_sent, "el_signed": total_el_signed,
        },
    })


@app.get("/api/executive/trend")
async def api_executive_trend(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 1.12 — Weekly Trend. Time-series of funnel stages + Meta spend."""
    import datetime as _dt
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    ds = _date.fromisoformat(d_since)
    du = _date.fromisoformat(d_until)
    total_days = (du - ds).days + 1
    weekly = total_days > 35

    def _bucket(date_str: str) -> str:
        """Return bucket key: YYYY-MM-DD of day, or Monday of week."""
        d = _date.fromisoformat(date_str[:10])
        if weekly:
            return (d - timedelta(days=d.weekday())).isoformat()
        return d.isoformat()

    # Build ordered bucket list
    buckets: list = []
    cur = ds
    while cur <= du:
        b = _bucket(cur.isoformat())
        if not buckets or buckets[-1] != b:
            buckets.append(b)
        cur += timedelta(days=1)

    agg: dict = {b: {"nl":0,"mb":0,"mh":0,"mql":0,"sql":0,"el_signed":0,"spend":0.0} for b in buckets}

    for c in _load_hs_new_leads(d_since, d_until, mkt_only=True):
        b = _bucket(c["date"])
        if b in agg: agg[b]["nl"] += 1

    for c in _load_hs_contacts(d_since, d_until, mkt_only=True):
        b = _bucket(c["date"])
        if b in agg:
            agg[b]["mb"] += 1
            if c.get("mql"):      agg[b]["mql"]      += 1
            if c.get("sql"):      agg[b]["sql"]      += 1
            if c.get("el_signed"):agg[b]["el_signed"] += 1

    for c in _load_hs_mh(d_since, d_until, mkt_only=True):
        b = _bucket(c["date"])
        if b in agg: agg[b]["mh"] += 1

    # Meta daily spend
    token = get_token(); accounts_list = get_accounts()
    if token and accounts_list:
        try:
            await _load_custom_conversions()
            for acc in accounts_list:
                daily_rows = await _get_daily_insights(acc, token, d_since, d_until)
                for row in daily_rows:
                    b = _bucket(row.get("date_start", "")[:10])
                    if b in agg:
                        agg[b]["spend"] += float(row.get("spend", 0))
        except Exception as e:
            print(f"[trend] spend fetch failed: {e}")

    series = [{"date": b, **agg[b], "spend": round(agg[b]["spend"], 2)} for b in buckets]

    return JSONResponse({
        "since": d_since, "until": d_until,
        "granularity": "week" if weekly else "day",
        "series": series,
    })


@app.get("/api/debug/hs-columns")
async def api_debug_hs_columns():
    """Returns all column names from the MB sheet — for debugging mismatches."""
    import csv as _csv, io as _io
    try:
        import httpx as _httpx
        r = await _httpx.AsyncClient().get(
            _HS_SHEET_BASE + _HS_GID_MB,
            follow_redirects=True, timeout=20
        )
        reader = _csv.DictReader(_io.StringIO(r.text))
        cols = reader.fieldnames or []
    except Exception as e:
        return JSONResponse({"error": str(e)})
    return JSONResponse({
        "columns": cols,
        "el_sent_col_expected": _HS_EL_SENT_COL,
        "el_sent_col_found": _HS_EL_SENT_COL in cols,
        "mh_col_expected": _HS_MH_COL,
        "mh_col_found": _HS_MH_COL in cols,
        "mb_date_col_expected": _HS_MB_DATE_COL,
        "mb_date_col_found": _HS_MB_DATE_COL in cols,
    })


@app.get("/executive", response_class=HTMLResponse)
async def executive(request: Request):
    return templates.TemplateResponse("executive.html", {"request": request})


@app.get("/api/hubspot/funnel")
async def api_hubspot_funnel(preset: str = "last_30d", since: str = None, until: str = None):
    cache_path = os.path.join(os.path.dirname(__file__), "hubspot_cache.json")
    if not os.path.exists(cache_path):
        raise HTTPException(status_code=503, detail="HubSpot cache not found. Run build_hs_cache.py first.")
    with open(cache_path, "r", encoding="utf-8") as f:
        cache = json.load(f)

    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    cache_min = cache.get("date_min", "2000-01-01")
    cache_max = cache.get("date_max", "2099-12-31")
    d_since = max(d_since, cache_min)
    d_until = min(d_until, cache_max)

    # All contacts in period — everyone here booked a meeting
    all_contacts = cache.get("contacts", cache.get("deals", []))
    contacts = [c for c in all_contacts if d_since <= c["date"] <= d_until]

    # Funnel: booked (everyone) → mql (MQL=Yes) → sql (SQL=Yes)
    booked = len(contacts)
    mql    = sum(1 for c in contacts if c.get("mql", False))
    sql    = sum(1 for c in contacts if c.get("sql", False))

    mql_rate = round(mql / booked * 100, 1) if booked else None
    sql_rate = round(sql / booked * 100, 1) if booked else None

    summary = {
        "booked":   booked,
        "mql":      mql,
        "sql":      sql,
        "mql_rate": mql_rate,
        "sql_rate": sql_rate,
    }

    # Breakdowns — ALL booked contacts
    by_camp: dict = {}
    by_cont: dict = {}
    by_attr: dict = {}

    for c in contacts:
        camp   = c.get("utm_campaign", "(no utm_campaign)")
        cont   = c.get("utm_content",  "(no utm_content)")
        attr   = c.get("attribution",  "(unknown)")
        is_mql = c.get("mql", False)
        is_sql = c.get("sql", False)

        for key, store in [(camp, by_camp), (cont, by_cont), (attr, by_attr)]:
            if key not in store:
                store[key] = {"label": key, "booked": 0, "mql": 0, "sql": 0}
            store[key]["booked"] += 1
            if is_mql: store[key]["mql"] += 1
            if is_sql: store[key]["sql"] += 1

    def _with_rates(store):
        out = []
        for item in store.values():
            b = item["booked"]
            item["mql_rate"] = round(item["mql"] / b * 100, 1) if b else None
            item["sql_rate"] = round(item["sql"] / b * 100, 1) if b else None
            out.append(item)
        return sorted(out, key=lambda x: -x["booked"])

    # Monthly trend — all booked contacts
    from collections import defaultdict
    monthly: dict = defaultdict(lambda: {"booked": 0, "mql": 0, "sql": 0})
    for c in contacts:
        mo = c["date"][:7]
        monthly[mo]["booked"] += 1
        if c.get("mql", False): monthly[mo]["mql"] += 1
        if c.get("sql", False): monthly[mo]["sql"] += 1
    monthly_list = [{"month": k, **v} for k, v in sorted(monthly.items())]

    return JSONResponse({
        "since": d_since, "until": d_until,
        "summary": summary,
        "by_campaign":    _with_rates(by_camp),
        "by_content":     _with_rates(by_cont),
        "by_attribution": _with_rates(by_attr),
        "monthly":        monthly_list,
        "cache_generated_at": cache.get("generated_at", ""),
    })


# ── Meta Ads Performance ──────────────────────────────────────────────────────

_META_ONSITE    = "Meta Ads - On-site Conversion"
_META_CALLINGLY = "Meta Ads - Callingly/Instant Forms"
_META_ATTRS     = {_META_ONSITE, _META_CALLINGLY}

def _camp_num(s: str):
    """Extract numeric campaign ID from strings like:
      '79_Leads_...'                         → '79'
      '[MARKETING] [SCHEDULES] [79]'         → '79'
      '[MARKETING] [INSTANT FORMS] [76] ...' → '76'
    Returns None if no number found.
    """
    m = re.match(r'^(\d+)[_\-]', s.strip())
    if m:
        return m.group(1)
    nums = re.findall(r'\[(\d+)\]', s)
    return nums[-1] if nums else None

def _build_canonical_by_num(meta_raw: dict) -> dict:
    """Build {campaign_number → canonical_display_name} from Meta API campaign names."""
    canonical: dict = {}
    for key, v in meta_raw.items():
        num = _camp_num(key)
        if num and re.match(r'^\d+_leads_', key):
            canonical[num] = v["name"]
    return canonical

def _resolve_canonical_key(s: str, canonical_by_num: dict) -> str:
    """Normalize any campaign string to its canonical number-based key, or itself."""
    num = _camp_num(s.lower())
    if num and num in canonical_by_num:
        return canonical_by_num[num].lower()
    return s.lower()

def _pct(num: int, den: int):
    return round(num / den * 100, 1) if den else None

def _meta_funnel_row(label: str, nl, mb, mh) -> dict:
    mql = [c for c in mb if c["mql"]]
    sql = [c for c in mb if c["sql"]]
    els = [c for c in mb if c["el_sent"]]
    eli = [c for c in mb if c["el_signed"]]
    return {
        "sub_channel":    label,
        "leads":          len(nl),
        "mb":             len(mb),
        "mb_rate":        _pct(len(mb),  len(nl)),
        "mql":            len(mql),
        "mql_rate":       _pct(len(mql), len(mb)),
        "mh":             len(mh),
        "show_rate":      _pct(len(mh),  len(mb)),
        "sql":            len(sql),
        "sql_rate":       _pct(len(sql), len(mb)),
        "el_sent":        len(els),
        "el_sent_rate":   _pct(len(els), len(sql)),
        "el_signed":      len(eli),
        "el_signed_rate": _pct(len(eli), len(els)),
    }


@app.get("/api/metaperf/funnel")
async def api_metaperf_funnel(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 2.1 — Meta Funnel Overview: On-Site vs Callingly."""
    await _load_custom_conversions()
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    nl_all = _load_hs_new_leads(d_since, d_until, mkt_only=False)
    mb_all = _load_hs_contacts(d_since, d_until, mkt_only=False)
    mh_all = _load_hs_mh(d_since, d_until, mkt_only=False)

    rows = []
    for attr_val, label in [(_META_ONSITE, "On-Site/Conversion"), (_META_CALLINGLY, "Callingly")]:
        rows.append(_meta_funnel_row(
            label,
            [c for c in nl_all if c["attribution"] == attr_val],
            [c for c in mb_all if c["attribution"] == attr_val],
            [c for c in mh_all if c["attribution"] == attr_val],
        ))

    # Meta Total = both sub-channels combined
    rows.append(_meta_funnel_row(
        "Meta Total",
        [c for c in nl_all if c["attribution"] in _META_ATTRS],
        [c for c in mb_all if c["attribution"] in _META_ATTRS],
        [c for c in mh_all if c["attribution"] in _META_ATTRS],
    ))

    # Add spend per sub-channel (campaigns with "instant" in name → Callingly)
    token = get_token(); accounts_list = get_accounts()
    sp_onsite = sp_callingly = 0.0
    if token and accounts_list:
        try:
            for acc in accounts_list:
                for row in await _get_insights(acc, token, d_since, d_until, "campaign"):
                    camp = row.get("campaign_name", "").lower()
                    sp = _row_metrics(row)["spend"]
                    if "instant" in camp:
                        sp_callingly += sp
                    else:
                        sp_onsite += sp
        except Exception as e:
            print(f"[metaperf/funnel] spend fetch failed: {e}")

    rows[0]["spend"] = round(sp_onsite, 2)
    rows[1]["spend"] = round(sp_callingly, 2)
    rows[2]["spend"] = round(sp_onsite + sp_callingly, 2)
    for r in rows:
        sp = r["spend"] or 0
        sql = r.get("sql") or 0
        r["cpsql"] = round(sp / sql, 2) if sp and sql else None

    return JSONResponse({"rows": rows, "since": d_since, "until": d_until})


@app.get("/api/metaperf/campaigns")
async def api_metaperf_campaigns(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 2.2 — Campaign-Level Breakdown (number-unified)."""
    await _load_custom_conversions()
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    # ── Meta API campaign-level data ──────────────────────────────────────────
    token = get_token()
    accounts_list = get_accounts()
    meta_raw: dict = {}   # campaign_name.lower() → {name, spend, impressions, link_clicks, leads}
    if token and accounts_list:
        for acc in accounts_list:
            for row in await _get_insights(acc, token, d_since, d_until, "campaign"):
                camp_name = row.get("campaign_name", "").strip()
                if not camp_name:
                    continue
                m = _row_metrics(row)
                key = camp_name.lower()
                if key in meta_raw:
                    for k in ("spend", "impressions", "link_clicks", "leads"):
                        meta_raw[key][k] += m[k]
                else:
                    meta_raw[key] = {"name": camp_name, "spend": m["spend"],
                                     "impressions": m["impressions"],
                                     "link_clicks": m["link_clicks"],
                                     "leads": m["leads"]}

    # Build canonical mapping: campaign number → canonical display name
    canonical_by_num = _build_canonical_by_num(meta_raw)
    def _canonical_key(s: str) -> str:
        return _resolve_canonical_key(s, canonical_by_num)

    # Merge Meta API data by canonical key
    meta_by_canon: dict = {}   # canonical_name.lower() → aggregated metrics
    for key, v in meta_raw.items():
        ck = _canonical_key(key)
        canon_name = canonical_by_num.get(_camp_num(key) or "", v["name"]) if ck in {c.lower() for c in canonical_by_num.values()} else v["name"]
        if ck not in meta_by_canon:
            meta_by_canon[ck] = {"name": canon_name, "spend": 0.0,
                                  "impressions": 0, "link_clicks": 0, "leads": 0}
        for k in ("spend", "impressions", "link_clicks", "leads"):
            meta_by_canon[ck][k] += v[k]
    # Recalculate derived fields
    for v in meta_by_canon.values():
        sp = v["spend"]; im = v["impressions"]; lc = v["link_clicks"]
        v["ctr"]   = round(lc / im * 100 if im else 0, 2)
        v["cpc"]   = round(sp / lc if lc else 0, 2)
        v["spend"] = round(sp, 2)

    # ── HubSpot data grouped by utm_campaign (Meta only) ─────────────────────
    nl_all = _load_hs_new_leads(d_since, d_until, mkt_only=False)
    mb_all = _load_hs_contacts(d_since, d_until, mkt_only=False)
    mh_all = _load_hs_mh(d_since, d_until, mkt_only=False)

    def _empty_hs() -> dict:
        return {"nl": 0, "mb": 0, "mql": 0, "mh": 0, "sql": 0, "el_sent": 0, "el_signed": 0}

    hs_by_canon: dict = {}   # canonical_key → bucket
    for c in nl_all:
        if c["attribution"] not in _META_ATTRS:
            continue
        ck = _canonical_key(c["utm_campaign"])
        if ck not in hs_by_canon:
            hs_by_canon[ck] = _empty_hs()
        hs_by_canon[ck]["nl"] += 1
    for c in mb_all:
        if c["attribution"] not in _META_ATTRS:
            continue
        ck = _canonical_key(c["utm_campaign"])
        if ck not in hs_by_canon:
            hs_by_canon[ck] = _empty_hs()
        hs_by_canon[ck]["mb"]       += 1
        if c["mql"]:       hs_by_canon[ck]["mql"]      += 1
        if c["sql"]:       hs_by_canon[ck]["sql"]       += 1
        if c["el_sent"]:   hs_by_canon[ck]["el_sent"]   += 1
        if c["el_signed"]: hs_by_canon[ck]["el_signed"] += 1
    for c in mh_all:
        if c["attribution"] not in _META_ATTRS:
            continue
        ck = _canonical_key(c["utm_campaign"])
        if ck not in hs_by_canon:
            hs_by_canon[ck] = _empty_hs()
        hs_by_canon[ck]["mh"] += 1

    # ── Build rows ────────────────────────────────────────────────────────────
    def _row(display_name: str, hs: dict, meta: dict) -> dict:
        sp  = meta.get("spend") or 0
        nl  = hs.get("nl", 0);  mb  = hs.get("mb", 0);  mql = hs.get("mql", 0)
        mh  = hs.get("mh", 0);  sql = hs.get("sql", 0)
        els = hs.get("el_sent", 0); eli = hs.get("el_signed", 0)
        def cp(den): return round(sp / den, 2) if sp and den else None
        return {
            "campaign":       display_name,
            "spend":          round(sp, 2) if sp else None,
            "impressions":    meta.get("impressions") or None,
            "link_clicks":    meta.get("link_clicks") or None,
            "ctr":            meta.get("ctr") or None,
            "cpc":            meta.get("cpc") or None,
            "leads_platform": meta.get("leads") or None,
            "leads_hs":       nl  or None,
            "cpl":            cp(nl),
            "mb":             mb  or None,
            "cpmb":           cp(mb),
            "mql":            mql or None,
            "cpmql":          cp(mql),
            "mh":             mh  or None,
            "cpmh":           cp(mh),
            "sql":            sql or None,
            "cpsql":          cp(sql),
            "el_sent":        els or None,
            "el_signed":      eli or None,
            "cost_el_signed": cp(eli),
        }

    all_keys = set(meta_by_canon.keys()) | set(hs_by_canon.keys())
    rows = []
    for ck in all_keys:
        meta_m = meta_by_canon.get(ck, {})
        hs_m   = hs_by_canon.get(ck, {})
        display = meta_m.get("name") or ck
        rows.append(_row(display, hs_m, meta_m))

    rows.sort(key=lambda r: r.get("spend") or 0, reverse=True)
    return JSONResponse({"rows": rows, "since": d_since, "until": d_until})


@app.get("/api/metaperf/adsets")
async def api_metaperf_adsets(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 2.3 — Ad Set-Level Breakdown."""
    await _load_custom_conversions()
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    token = get_token()
    accounts_list = get_accounts()

    # ── Meta API adset insights + metadata ────────────────────────────────────
    # Build canonical_by_num from campaign-level insights for number-based merging
    meta_adsets: dict = {}
    canonical_by_num: dict = {}
    if token and accounts_list:
        _camp_raw: dict = {}
        for acc in accounts_list:
            for row in await _get_insights(acc, token, d_since, d_until, "campaign"):
                cn = row.get("campaign_name", "").strip()
                if cn:
                    _camp_raw[cn.lower()] = {"name": cn}
        canonical_by_num = _build_canonical_by_num(_camp_raw)
        def _canonical_key(s: str) -> str:
            return _resolve_canonical_key(s, canonical_by_num)

        meta_meta: dict = {}  # adset_name → metadata (opt_goal, daily_budget)
        for acc in accounts_list:
            cdata = await _fetch_campaign_data(acc, token, "adset")
            meta_meta.update(cdata)
            for row in await _get_insights(acc, token, d_since, d_until, "adset"):
                adset_name = row.get("adset_name", "").strip()
                camp_name  = row.get("campaign_name", "").strip()
                if not adset_name:
                    continue
                m   = _row_metrics(row)
                ck  = _canonical_key(camp_name.lower())
                key = (ck, adset_name.lower())
                if key in meta_adsets:
                    for k in ("spend", "impressions", "link_clicks", "leads"):
                        meta_adsets[key][k] += m[k]
                else:
                    meta = meta_meta.get(adset_name, {})
                    db_raw = meta.get("daily_budget")
                    db = round(float(db_raw) / 100, 2) if db_raw else None
                    meta_adsets[key] = {
                        "adset_name":      adset_name,
                        "campaign_name":   camp_name,
                        "opt_goal":        meta.get("optimization_goal", ""),
                        "daily_budget":    db,
                        "spend":           m["spend"],
                        "impressions":     m["impressions"],
                        "link_clicks":     m["link_clicks"],
                        "leads":           m["leads"],
                    }
        for v in meta_adsets.values():
            sp = v["spend"]; im = v["impressions"]; lc = v["link_clicks"]
            v["ctr"]   = round(lc / im * 100 if im else 0, 2)
            v["cpc"]   = round(sp / lc if lc else 0, 2)
            v["spend"] = round(sp, 2)

    # Ensure _canonical_key exists even when token is absent
    if not canonical_by_num:
        def _canonical_key(s: str) -> str:  # noqa: F811
            return s.lower()

    # ── HubSpot data grouped by (canonical_campaign_key, utm_content.lower()) ─
    nl_all = _load_hs_new_leads(d_since, d_until, mkt_only=False)
    mb_all = _load_hs_contacts(d_since, d_until, mkt_only=False)
    mh_all = _load_hs_mh(d_since, d_until, mkt_only=False)

    hs_adsets: dict = {}
    def _hs_key(c):
        return (_canonical_key(c["utm_campaign"]), c.get("utm_medium", "").lower())

    for c in nl_all:
        if c["attribution"] not in _META_ATTRS: continue
        k = _hs_key(c)
        if k not in hs_adsets: hs_adsets[k] = {"nl":0,"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
        hs_adsets[k]["nl"] += 1
    for c in mb_all:
        if c["attribution"] not in _META_ATTRS: continue
        k = _hs_key(c)
        if k not in hs_adsets: hs_adsets[k] = {"nl":0,"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
        hs_adsets[k]["mb"]       += 1
        if c["mql"]:       hs_adsets[k]["mql"]      += 1
        if c["sql"]:       hs_adsets[k]["sql"]       += 1
        if c["el_sent"]:   hs_adsets[k]["el_sent"]   += 1
        if c["el_signed"]: hs_adsets[k]["el_signed"] += 1
    for c in mh_all:
        if c["attribution"] not in _META_ATTRS: continue
        k = _hs_key(c)
        if k not in hs_adsets: hs_adsets[k] = {"nl":0,"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
        hs_adsets[k]["mh"] += 1

    # ── Build rows ────────────────────────────────────────────────────────────
    all_keys = set(meta_adsets.keys()) | set(hs_adsets.keys())
    rows = []
    for key in all_keys:
        meta_m = meta_adsets.get(key, {})
        hs_m   = hs_adsets.get(key, {})
        sp  = meta_m.get("spend") or 0
        nl  = hs_m.get("nl",0); mb  = hs_m.get("mb",0);  mql = hs_m.get("mql",0)
        mh  = hs_m.get("mh",0); sql = hs_m.get("sql",0)
        els = hs_m.get("el_sent",0); eli = hs_m.get("el_signed",0)
        def cp(den): return round(sp / den, 2) if sp and den else None
        rows.append({
            "campaign":       meta_m.get("campaign_name") or key[0],
            "adset_name":     meta_m.get("adset_name") or key[1],
            "opt_goal":       meta_m.get("opt_goal") or None,
            "daily_budget":   meta_m.get("daily_budget"),
            "spend":          round(sp, 2) if sp else None,
            "impressions":    meta_m.get("impressions") or None,
            "link_clicks":    meta_m.get("link_clicks") or None,
            "ctr":            meta_m.get("ctr") or None,
            "cpc":            meta_m.get("cpc") or None,
            "leads_platform": meta_m.get("leads") or None,
            "leads_hs":       nl  or None,
            "cpl":            cp(nl),
            "mb":             mb  or None,
            "cpmb":           cp(mb),
            "mql":            mql or None,
            "cpmql":          cp(mql),
            "mh":             mh  or None,
            "cpmh":           cp(mh),
            "sql":            sql or None,
            "cpsql":          cp(sql),
            "el_sent":        els or None,
            "el_signed":      eli or None,
            "cost_el_signed": cp(eli),
        })

    rows.sort(key=lambda r: r.get("spend") or 0, reverse=True)
    return JSONResponse({"rows": rows, "since": d_since, "until": d_until})


@app.get("/api/metaperf/ads")
async def api_metaperf_ads(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 2.4 — Creative (Ad)-Level Breakdown. Join key: utm_content = ad_name."""
    await _load_custom_conversions()
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )

    token = get_token()
    accounts_list = get_accounts()

    # ── Meta API ad-level insights ────────────────────────────────────────────
    meta_ads: dict = {}   # ad_name.lower() → metrics
    if token and accounts_list:
        for acc in accounts_list:
            for row in await _get_insights(acc, token, d_since, d_until, "ad"):
                ad_name = row.get("ad_name", "").strip()
                if not ad_name:
                    continue
                m   = _row_metrics(row)
                key = ad_name.lower()
                if key in meta_ads:
                    for k in ("spend", "impressions", "link_clicks", "leads", "video_views"):
                        meta_ads[key][k] += m[k]
                else:
                    meta_ads[key] = {
                        "ad_name":     ad_name,
                        "spend":       m["spend"],
                        "impressions": m["impressions"],
                        "link_clicks": m["link_clicks"],
                        "leads":       m["leads"],
                        "video_views": m["video_views"],
                    }
        for v in meta_ads.values():
            sp = v["spend"]; im = v["impressions"]; lc = v["link_clicks"]; vv = v["video_views"]
            v["ctr"]       = round(lc / im * 100 if im else 0, 2)
            v["cpc"]       = round(sp / lc if lc else 0, 2)
            v["hook_rate"] = round(vv / im * 100 if im and vv else 0, 2) or None
            v["spend"]     = round(sp, 2)

    # ── HubSpot data grouped by utm_content (Meta only) ──────────────────────
    nl_all = _load_hs_new_leads(d_since, d_until, mkt_only=False)
    mb_all = _load_hs_contacts(d_since, d_until, mkt_only=False)
    mh_all = _load_hs_mh(d_since, d_until, mkt_only=False)

    hs_by_content: dict = {}
    def _ck(c): return c.get("utm_content", "").lower()

    for c in nl_all:
        if c["attribution"] not in _META_ATTRS: continue
        k = _ck(c)
        if k not in hs_by_content: hs_by_content[k] = {"nl":0,"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
        hs_by_content[k]["nl"] += 1
    for c in mb_all:
        if c["attribution"] not in _META_ATTRS: continue
        k = _ck(c)
        if k not in hs_by_content: hs_by_content[k] = {"nl":0,"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
        hs_by_content[k]["mb"]       += 1
        if c["mql"]:       hs_by_content[k]["mql"]      += 1
        if c["sql"]:       hs_by_content[k]["sql"]       += 1
        if c["el_sent"]:   hs_by_content[k]["el_sent"]   += 1
        if c["el_signed"]: hs_by_content[k]["el_signed"] += 1
    for c in mh_all:
        if c["attribution"] not in _META_ATTRS: continue
        k = _ck(c)
        if k not in hs_by_content: hs_by_content[k] = {"nl":0,"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0,"el_signed":0}
        hs_by_content[k]["mh"] += 1

    # ── Build rows ────────────────────────────────────────────────────────────
    all_keys = set(meta_ads.keys()) | set(hs_by_content.keys())
    rows = []
    for key in all_keys:
        meta_m = meta_ads.get(key, {})
        hs_m   = hs_by_content.get(key, {})
        sp  = meta_m.get("spend") or 0
        nl  = hs_m.get("nl",0);  mb  = hs_m.get("mb",0);  mql = hs_m.get("mql",0)
        mh  = hs_m.get("mh",0);  sql = hs_m.get("sql",0)
        els = hs_m.get("el_sent",0); eli = hs_m.get("el_signed",0)
        def cp(den): return round(sp / den, 2) if sp and den else None
        rows.append({
            "ad_name":        meta_m.get("ad_name") or key,
            "hook_rate":      meta_m.get("hook_rate"),
            "spend":          round(sp, 2) if sp else None,
            "impressions":    meta_m.get("impressions") or None,
            "link_clicks":    meta_m.get("link_clicks") or None,
            "ctr":            meta_m.get("ctr") or None,
            "cpc":            meta_m.get("cpc") or None,
            "leads_platform": meta_m.get("leads") or None,
            "leads_hs":       nl  or None,
            "cpl":            cp(nl),
            "mb":             mb  or None,
            "cpmb":           cp(mb),
            "mql":            mql or None,
            "cpmql":          cp(mql),
            "mh":             mh  or None,
            "cpmh":           cp(mh),
            "sql":            sql or None,
            "cpsql":          cp(sql),
            "el_sent":        els or None,
            "el_signed":      eli or None,
            "cost_el_signed": cp(eli),
        })

    rows.sort(key=lambda r: r.get("spend") or 0, reverse=True)
    return JSONResponse({"rows": rows, "since": d_since, "until": d_until})


@app.get("/api/metaperf/trend")
async def api_metaperf_trend(preset: str = "this_month", since: str = None, until: str = None):
    """Panel 2.6 — Daily/Weekly Trend filtered to Meta attribution."""
    await _load_custom_conversions()
    d_since, d_until, _, _ = _compute_period(
        preset if not (since and until) else None, since, until
    )
    ds = _date.fromisoformat(d_since)
    du = _date.fromisoformat(d_until)
    total_days = (du - ds).days + 1
    weekly = total_days > 35

    def _bucket(date_str: str) -> str:
        d = _date.fromisoformat(date_str[:10])
        if weekly:
            return (d - timedelta(days=d.weekday())).isoformat()
        return d.isoformat()

    buckets: list = []
    cur = ds
    while cur <= du:
        b = _bucket(cur.isoformat())
        if not buckets or buckets[-1] != b:
            buckets.append(b)
        cur += timedelta(days=1)

    agg: dict = {b: {"nl":0,"mb":0,"mh":0,"mql":0,"sql":0,"el_signed":0,"spend":0.0} for b in buckets}

    for c in _load_hs_new_leads(d_since, d_until, mkt_only=False):
        if c["attribution"] not in _META_ATTRS: continue
        b = _bucket(c["date"])
        if b in agg: agg[b]["nl"] += 1

    for c in _load_hs_contacts(d_since, d_until, mkt_only=False):
        if c["attribution"] not in _META_ATTRS: continue
        b = _bucket(c["date"])
        if b in agg:
            agg[b]["mb"] += 1
            if c.get("mql"):       agg[b]["mql"]       += 1
            if c.get("sql"):       agg[b]["sql"]        += 1
            if c.get("el_signed"): agg[b]["el_signed"]  += 1

    for c in _load_hs_mh(d_since, d_until, mkt_only=False):
        if c["attribution"] not in _META_ATTRS: continue
        b = _bucket(c["date"])
        if b in agg: agg[b]["mh"] += 1

    token = get_token(); accounts_list = get_accounts()
    if token and accounts_list:
        try:
            for acc in accounts_list:
                for row in await _get_daily_insights(acc, token, d_since, d_until):
                    b = _bucket(row.get("date_start", "")[:10])
                    if b in agg:
                        agg[b]["spend"] += float(row.get("spend", 0))
        except Exception as e:
            print(f"[metaperf/trend] spend fetch failed: {e}")

    series = [{"date": b, **agg[b], "spend": round(agg[b]["spend"], 2)} for b in buckets]
    return JSONResponse({
        "since": d_since, "until": d_until,
        "granularity": "week" if weekly else "day",
        "series": series,
    })
