import os
import io
import csv
import json
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
    name_f = {"campaign": "campaign_name", "adset": "adset_name"}.get(level, "")
    extra  = ",campaign_name" if level == "adset" else ""
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
    async with httpx.AsyncClient() as c:
        try:
            r = await c.get(f"{META_GRAPH_BASE}/{account_id}/{endpoint}", params={
                "fields": "id,name,daily_budget,lifetime_budget,effective_status",
                "access_token": token,
                "limit": 200,
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
    "Meta Ads - On-site/Conversion",   # actual HubSpot value (lowercase s, hyphen)
    "Meta Ads - On Site/Conversion",   # keep variant just in case
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
_HS_SHEET_BASE  = "https://docs.google.com/spreadsheets/d/1szR5aHU5j1FijE4mBVmlx2A0AsA7-lvocgsbO6UFmCw/export?format=csv&gid="
_HS_GID_MB      = "1408761440"
_HS_MB_DATE_COL = "Date entered \"Meeting Scheduled (Placements — Inbound Sales Stage)\""
_HS_MH_COL      = "Meeting Start Time"
_HS_EL_SENT_COL = "Date Engagement Letter Was Sent"
_HS_CACHE_TTL   = 900  # seconds (15 min)
_HS_PLACEHOLDERS = {"{{campaign.name}}", "{{ad.name}}", ""}

_hs_mem: dict = {"contacts": [], "loaded_at": 0.0}

def _clean_utm(val: str) -> str:
    if not val:
        return ""
    try:
        val = urllib.parse.unquote_plus(val)
    except Exception:
        pass
    val = val.strip()
    return "" if val in _HS_PLACEHOLDERS else val

def _fetch_hs_contacts() -> list:
    """Download MB sheet from Google Sheets and return parsed contacts list."""
    url = _HS_SHEET_BASE + _HS_GID_MB
    with urllib.request.urlopen(url, timeout=20) as resp:
        content = resp.read().decode("utf-8")
    contacts = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        raw_date = (row.get(_HS_MB_DATE_COL, "") or row.get("Create Date", "")).strip()
        date = raw_date[:10]
        if not date or len(date) < 10:
            continue
        email       = row.get("Email", "").strip().lower()
        mql_val     = row.get("MQL", "").strip()
        sql_val     = row.get("SQL", "").strip()
        attribution = row.get("Attribution (Contact-Level)", "").strip() or "(unknown)"
        mh          = bool(row.get(_HS_MH_COL, "").strip())
        el_sent     = bool(row.get(_HS_EL_SENT_COL, "").strip())
        contacts.append({
            "date":         date,
            "email":        email,
            "mql":          mql_val == "Yes",
            "sql":          sql_val == "Yes",
            "mh":           mh,
            "el_sent":      el_sent,
            "attribution":  attribution,
            "utm_source":   _clean_utm(row.get("utm_source",   "")) or "",
            "utm_campaign": _clean_utm(row.get("utm_campaign", "")) or "(no utm_campaign)",
            "utm_content":  _clean_utm(row.get("utm_content",  "")) or "(no utm_content)",
        })
    contacts.sort(key=lambda x: x["date"])
    return contacts

def _get_hs_contacts() -> list:
    """Return contacts from memory cache, refreshing from Sheets if stale."""
    now = time.time()
    if now - _hs_mem["loaded_at"] > _HS_CACHE_TTL:
        try:
            _hs_mem["contacts"]  = _fetch_hs_contacts()
            _hs_mem["loaded_at"] = now
        except Exception as e:
            # Keep stale data if fetch fails; log error
            print(f"[HS] Sheet fetch failed: {e}")
            if not _hs_mem["contacts"]:
                # Last resort: try reading from local cache file if it exists
                _hs_cache_path = os.path.join(os.path.dirname(__file__), "hubspot_cache.json")
                if os.path.exists(_hs_cache_path):
                    with open(_hs_cache_path, "r", encoding="utf-8") as f:
                        _hs_mem["contacts"] = json.load(f).get("contacts", [])
    return _hs_mem["contacts"]

def _load_hs_contacts(d_since: str, d_until: str, mkt_only: bool = True):
    """Return contacts filtered by date range and optionally MKT attribution."""
    all_c = _get_hs_contacts()
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
    contacts = _load_hs_contacts(d_since, d_until, mkt_only=True)

    mb      = len(contacts)
    mql     = sum(1 for c in contacts if c.get("mql"))
    mh      = sum(1 for c in contacts if c.get("mh"))
    sql     = sum(1 for c in contacts if c.get("sql"))
    el_sent = sum(1 for c in contacts if c.get("el_sent"))

    def _pct_of_mb(num):
        return round(num / mb * 100, 1) if mb else None

    stages = [
        {"stage": "Meeting Booked (MB)", "count": mb,      "conv_from_mb": None},
        {"stage": "MQL",                 "count": mql,     "conv_from_mb": _pct_of_mb(mql)},
        {"stage": "Meeting Held (MH)",   "count": mh,      "conv_from_mb": _pct_of_mb(mh)},
        {"stage": "SQL",                 "count": sql,     "conv_from_mb": _pct_of_mb(sql)},
        {"stage": "EL Sent",             "count": el_sent, "conv_from_mb": _pct_of_mb(el_sent)},
        {"stage": "EL Signed",           "count": None,    "conv_from_mb": None},
    ]

    return JSONResponse({
        "since": d_since, "until": d_until,
        "stages": stages,
        "totals": {"mb": mb, "mql": mql, "mh": mh, "sql": sql, "el_sent": el_sent},
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
    contacts = _load_hs_contacts(d_since, d_until, mkt_only=True)

    # Accumulators per channel key
    channels: dict = {}
    def _ensure(key):
        if key not in channels:
            channels[key] = {"mb": 0, "mql": 0, "mh": 0, "sql": 0, "el_sent": 0}
        return channels[key]

    for c in contacts:
        ch = _attr_channel(c.get("attribution", ""), c.get("utm_campaign", ""))
        _ensure(ch)
        channels[ch]["mb"]  += 1
        if c.get("mql"):     channels[ch]["mql"]     += 1
        if c.get("mh"):      channels[ch]["mh"]      += 1
        if c.get("sql"):     channels[ch]["sql"]      += 1
        if c.get("el_sent"): channels[ch]["el_sent"]  += 1

    def _cost(spend, count):
        return round(spend / count, 2) if count else None

    def _row(label, spend, ch_key, sub=False):
        d = channels.get(ch_key, {"mb": 0, "mql": 0, "mh": 0, "sql": 0, "el_sent": 0})
        mb, mql, mh, sql, el_sent = d["mb"], d["mql"], d["mh"], d["sql"], d["el_sent"]
        return {
            "channel": label,
            "sub": sub,
            "spend": spend,
            "mb":      mb,      "cpmb":          _cost(spend, mb)      if spend else None,
            "mql":     mql,     "cpmql":         _cost(spend, mql)     if spend else None,
            "mh":      mh,      "cpmh":          _cost(spend, mh)      if spend else None,
            "sql":     sql,     "cpsql":         _cost(spend, sql)     if spend else None,
            "el_sent": el_sent, "cost_el_sent":  _cost(spend, el_sent) if spend else None,
            "el_signed": None,  "cost_el_signed": None,
        }

    # Total MKT funnel counts (same source as Panel 1.1)
    total_mb      = sum(1 for c in contacts)
    total_mql     = sum(1 for c in contacts if c.get("mql"))
    total_mh      = sum(1 for c in contacts if c.get("mh"))
    total_sql     = sum(1 for c in contacts if c.get("sql"))
    total_el_sent = sum(1 for c in contacts if c.get("el_sent"))

    # Meta-attributed counts for the Meta Ads row
    _zero = {"mb":0,"mql":0,"mh":0,"sql":0,"el_sent":0}
    meta_onsite    = channels.get("meta_onsite",    _zero)
    meta_callingly = channels.get("meta_callingly", _zero)
    meta_mb      = meta_onsite["mb"]      + meta_callingly["mb"]
    meta_mql     = meta_onsite["mql"]     + meta_callingly["mql"]
    meta_mh      = meta_onsite["mh"]      + meta_callingly["mh"]
    meta_sql     = meta_onsite["sql"]     + meta_callingly["sql"]
    meta_el_sent = meta_onsite["el_sent"] + meta_callingly["el_sent"]

    rows = [
        {
            "channel": "Meta Ads", "sub": False,
            "spend": meta_spend,
            "mb":      meta_mb,      "cpmb":         _cost(meta_spend, meta_mb),
            "mql":     meta_mql,     "cpmql":        _cost(meta_spend, meta_mql),
            "mh":      meta_mh,      "cpmh":         _cost(meta_spend, meta_mh),
            "sql":     meta_sql,     "cpsql":        _cost(meta_spend, meta_sql),
            "el_sent": meta_el_sent, "cost_el_sent": _cost(meta_spend, meta_el_sent),
            "el_signed": None, "cost_el_signed": None,
        },
        _row(">> On-Site/Conversion",       spend_onsite,    "meta_onsite",    sub=True),
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
        "el_sent": total_el_sent, "cost_el_sent": _cost(meta_spend, total_el_sent),
        "el_signed": None, "cost_el_signed": None,
    })

    return JSONResponse({
        "since": d_since, "until": d_until,
        "rows": rows,
        "meta_spend_total": meta_spend,
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
