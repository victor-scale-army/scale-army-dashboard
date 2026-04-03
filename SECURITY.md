# Security Documentation — Meta Ads Dashboard

**Project:** Scale Army Meta Ads Dashboard
**Technical Lead:** Victor Dognini
**Date:** April 2026

---

## What this project is

An internal web application for visualizing Meta Ads (Facebook/Instagram) campaign metrics for Scale Army. It displays investment, reach, clicks, leads, and creative data in real time, directly from the official Meta API.

---

## Architecture

```
User's browser
      ↕  HTTPS
Server (Python/FastAPI)
      ↕  HTTPS
Meta Graph API (graph.facebook.com)
```

**Stack:**
- Backend: Python 3 + FastAPI
- Frontend: Plain HTML/CSS/JS (no external frameworks)
- Hosting: TBD (own VPS or Scale Army domain) or Vercel
- Database: **none** — data is not stored locally

---

## Meta API Access

### Access type
- **Read-only**
- Permission used: `ads_read`
- No write permissions requested or granted (`ads_management` is not used)

### What is read
| Meta API Endpoint | Data accessed |
|---|---|
| `/{account_id}/insights` | Performance metrics (spend, impressions, clicks, leads) |
| `/{ad_id}` | Creative thumbnail and link (image/video) |
| `/{account_id}` | Account name |

### What is **not** done
- Creating, editing, or deleting campaigns
- Accessing user/audience data
- Accessing messages, pages, or other assets
- Storing data in a database or file

### Authentication
- **System User Token** generated in Scale Army's Meta Business Manager
- Token stored as an environment variable on the server (never exposed in the frontend or in code)
- Token is scoped to Scale Army's Meta account only

---

## External tools and services

| Service | Usage | Data sent |
|---|---|---|
| Meta Graph API | Data source | None — read-only queries |
| Chart.js (jsDelivr CDN) | Frontend charts | None — static JS library |

**No analytics, tracking, or third-party service receives ad data.**

---

## Application access

- URL accessible only to Scale Army's internal team
- No login system by default (access via internal URL)
- Recommendation: restrict by IP or add basic authentication before public deployment

---

## Approval checklist

- [x] Clear description of purpose
- [x] Access type: read-only
- [x] Which data is queried
- [x] Which data is **not** accessed or stored
- [x] No external service receives data
- [x] Secure authentication token (environment variable)
- [x] Auditable stack (open source, no opaque dependencies)

---

## Technical contact

Victor Dognini — victor@scalearmy.com
