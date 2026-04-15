"""
Builds hubspot_cache.json by joining deals (hs_deals_raw.json) with contacts
fetched from HubSpot via MCP, using date-proximity matching.
"""
import json
import datetime
import urllib.parse
from pathlib import Path

BASE = Path(__file__).parent

# ---------------------------------------------------------------------------
# Stage classification
# ---------------------------------------------------------------------------
STAGE_SCHEDULED = "1162444910"
STAGE_NOSH      = "1162732907"
STAGES_HELD     = {"1162444911", "1162444912", "1162444913", "1162785708", "1203929600"}

def classify(stage: str) -> str:
    if stage == STAGE_NOSH:      return "no_show"
    if stage == STAGE_SCHEDULED: return "scheduled"
    if stage in STAGES_HELD:     return "held"
    return "held"  # unknown → held

# ---------------------------------------------------------------------------
# All contacts from all 7 batches (deduplicated by id)
# Each: {id, createdate, utm_campaign, utm_content}
# ---------------------------------------------------------------------------
RAW_CONTACTS = [
    # ── BATCH 1 (deals Jan 01–19) ──────────────────────────────────────────
    {"id":"194416634385","createdate":"2026-01-19T15:27:53.358Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"194257588338","createdate":"2026-01-19T04:58:44.517Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194140300870","createdate":"2026-01-18T15:58:58.800Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194132933904","createdate":"2026-01-18T14:43:42.307Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"192006537066","createdate":"2026-01-18T14:32:32.371Z","utm_campaign":"[MARKETING] [SCHEDULES] [61]","utm_content":"[MARKETING MANAGER] [CHAOS] [01]"},
    {"id":"194118035358","createdate":"2026-01-18T13:31:04.158Z","utm_campaign":"[MARKETING] [SCHEDULES] [08]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"188293794828","createdate":"2026-01-17T17:44:56.516Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"193966829185","createdate":"2026-01-17T15:04:50.474Z","utm_campaign":"[MARKETING] [SCHEDULES] [61]","utm_content":"[MARKETING MANAGER] [CHAOS] [01]"},
    {"id":"193882678949","createdate":"2026-01-17T12:48:47.567Z","utm_campaign":"","utm_content":""},
    {"id":"193951949854","createdate":"2026-01-17T12:48:42.646Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"193871128113","createdate":"2026-01-17T00:18:41.197Z","utm_campaign":"","utm_content":""},
    {"id":"193810551122","createdate":"2026-01-16T18:54:55.844Z","utm_campaign":"","utm_content":""},
    {"id":"193709412174","createdate":"2026-01-16T13:11:45.603Z","utm_campaign":"","utm_content":""},
    {"id":"193708435484","createdate":"2026-01-16T12:44:38.909Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"193622334508","createdate":"2026-01-16T07:16:02.051Z","utm_campaign":"","utm_content":""},
    {"id":"193545096195","createdate":"2026-01-15T22:07:55.856Z","utm_campaign":"","utm_content":""},
    {"id":"193511862808","createdate":"2026-01-15T21:17:26.968Z","utm_campaign":"[MARKETING] [SCHEDULES] [08]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"193413228707","createdate":"2026-01-15T14:20:06.132Z","utm_campaign":"","utm_content":""},
    {"id":"193260204157","createdate":"2026-01-15T00:20:37.105Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"193154587039","createdate":"2026-01-14T20:07:22.847Z","utm_campaign":"[GENERAL][LAL][SCHEDULE][01]","utm_content":"[OFFSHORE][SOCIALMEDIA][BLACKNOTE][04]"},
    {"id":"192868666278","createdate":"2026-01-14T01:35:09.214Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"192842986776","createdate":"2026-01-14T01:28:24.162Z","utm_campaign":"","utm_content":""},
    {"id":"192863462057","createdate":"2026-01-14T00:25:20.571Z","utm_campaign":"[MARKETING] [SCHEDULES] [61]","utm_content":"[MARKETING MANAGER] [CHAOS] [01]"},
    {"id":"184215327250","createdate":"2026-01-13T23:04:57.222Z","utm_campaign":"","utm_content":""},
    {"id":"192798351462","createdate":"2026-01-13T22:04:15.456Z","utm_campaign":"[MARKETING] [SCHEDULES] [08]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"192818265666","createdate":"2026-01-13T21:18:59.659Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"192620015948","createdate":"2026-01-13T13:09:58.138Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"192465331024","createdate":"2026-01-12T22:56:35.548Z","utm_campaign":"","utm_content":""},
    {"id":"192351913492","createdate":"2026-01-12T18:28:57.409Z","utm_campaign":"","utm_content":""},
    {"id":"192125030687","createdate":"2026-01-12T14:10:30.104Z","utm_campaign":"","utm_content":""},
    {"id":"192304454990","createdate":"2026-01-12T13:52:25.749Z","utm_campaign":"","utm_content":""},
    {"id":"192273336364","createdate":"2026-01-12T13:52:25.337Z","utm_campaign":"","utm_content":""},
    {"id":"192303846304","createdate":"2026-01-12T13:52:25.244Z","utm_campaign":"","utm_content":""},
    {"id":"192274061866","createdate":"2026-01-12T13:52:25.203Z","utm_campaign":"","utm_content":""},
    {"id":"191918721430","createdate":"2026-01-10T19:32:42.925Z","utm_campaign":"[GENERAL][LAL][SCHEDULE][01]","utm_content":"[OFFSHORE][SOCIALMEDIA][BLACKNOTE][04]"},
    {"id":"191900313623","createdate":"2026-01-10T17:16:22.971Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"191735691075","createdate":"2026-01-10T07:14:24.877Z","utm_campaign":"","utm_content":""},
    {"id":"191720601621","createdate":"2026-01-09T21:39:28.636Z","utm_campaign":"[MARKETING] [SCHEDULES] [65]","utm_content":"[JERRICA] [PLAIN] [FRIEND] [01]"},
    {"id":"191742422618","createdate":"2026-01-09T20:18:48.093Z","utm_campaign":"","utm_content":""},
    {"id":"191679197104","createdate":"2026-01-09T16:32:52.113Z","utm_campaign":"","utm_content":""},
    {"id":"191549758309","createdate":"2026-01-09T05:49:58.317Z","utm_campaign":"","utm_content":""},
    {"id":"191493504905","createdate":"2026-01-08T23:28:37.199Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"191466740614","createdate":"2026-01-08T21:32:22.400Z","utm_campaign":"","utm_content":""},
    {"id":"191438911087","createdate":"2026-01-08T19:00:56.582Z","utm_campaign":"[MARKETING] [SCHEDULES] [61]","utm_content":"[MARKETING MANAGER] [CHAOS] [01]"},
    {"id":"191373379637","createdate":"2026-01-08T13:56:52.465Z","utm_campaign":"","utm_content":""},
    {"id":"191185325095","createdate":"2026-01-08T06:17:53.634Z","utm_campaign":"","utm_content":""},
    {"id":"191212781110","createdate":"2026-01-07T21:15:48.353Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"191097239115","createdate":"2026-01-07T13:40:27.568Z","utm_campaign":"","utm_content":""},
    {"id":"191102474055","createdate":"2026-01-07T13:40:21.998Z","utm_campaign":"","utm_content":""},
    {"id":"191026995002","createdate":"2026-01-07T05:20:18.792Z","utm_campaign":"","utm_content":"link_in_bio"},
    {"id":"191018474524","createdate":"2026-01-07T03:27:51.003Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"191020598549","createdate":"2026-01-07T02:59:39.714Z","utm_campaign":"","utm_content":""},
    {"id":"191008459378","createdate":"2026-01-07T02:13:11.920Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"191016163119","createdate":"2026-01-07T01:46:45.482Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"190924387646","createdate":"2026-01-06T18:33:47.888Z","utm_campaign":"","utm_content":""},
    {"id":"190907088151","createdate":"2026-01-06T15:58:28.134Z","utm_campaign":"[MARKETING] [SCHEDULES] [61]","utm_content":"[MARKETING MANAGER] [CHAOS] [01]"},
    {"id":"190902982446","createdate":"2026-01-06T15:05:46.308Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"190819823147","createdate":"2026-01-06T12:09:19.523Z","utm_campaign":"","utm_content":""},
    {"id":"190826361995","createdate":"2026-01-06T12:09:19.418Z","utm_campaign":"","utm_content":""},
    {"id":"190599824910","createdate":"2026-01-06T12:09:19.340Z","utm_campaign":"","utm_content":""},
    {"id":"190836629931","createdate":"2026-01-06T12:09:19.326Z","utm_campaign":"","utm_content":""},
    {"id":"190737965961","createdate":"2026-01-06T06:16:57.511Z","utm_campaign":"","utm_content":""},
    {"id":"190657617496","createdate":"2026-01-06T01:46:25.804Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"183194454815","createdate":"2026-01-06T00:36:51.205Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"190388637572","createdate":"2026-01-05T17:24:41.997Z","utm_campaign":"","utm_content":""},
    {"id":"190490801970","createdate":"2026-01-05T14:46:45.431Z","utm_campaign":"[MARKETING] [SCHEDULES] [65]","utm_content":"[JERRICA] [PLAIN] [FRIEND] [01]"},
    {"id":"190387776381","createdate":"2026-01-05T12:43:10.797Z","utm_campaign":"","utm_content":""},
    {"id":"190467026983","createdate":"2026-01-05T12:25:10.421Z","utm_campaign":"","utm_content":""},
    {"id":"149712509307","createdate":"2026-01-05T02:27:33.793Z","utm_campaign":"[SALES] [MARKETING] [SCHEDULES] [RETARGETING] [55]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"190342043188","createdate":"2026-01-05T02:15:06.960Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"190316399780","createdate":"2026-01-04T22:39:56.980Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"190185982535","createdate":"2026-01-04T04:57:05.858Z","utm_campaign":"","utm_content":""},
    {"id":"190085922102","createdate":"2026-01-03T12:10:34.495Z","utm_campaign":"[GENERAL][LAL][SCHEDULE][33]","utm_content":""},
    {"id":"190004891253","createdate":"2026-01-03T03:59:32.939Z","utm_campaign":"","utm_content":""},
    {"id":"189990085524","createdate":"2026-01-03T03:57:31.108Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"189996595737","createdate":"2026-01-03T02:41:40.985Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"189947306577","createdate":"2026-01-02T22:24:27.293Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"189810569632","createdate":"2026-01-02T11:31:02.121Z","utm_campaign":"","utm_content":""},
    {"id":"189705784892","createdate":"2026-01-02T02:14:58.493Z","utm_campaign":"","utm_content":""},
    {"id":"189564658285","createdate":"2026-01-01T17:16:40.962Z","utm_campaign":"","utm_content":""},
    {"id":"189616574577","createdate":"2026-01-01T14:46:58.648Z","utm_campaign":"[MARKETING] [SCHEDULES] [61]","utm_content":"[MARKETING MANAGER] [CHAOS] [01]"},
    {"id":"189610063203","createdate":"2026-01-01T13:50:35.195Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"189479617662","createdate":"2025-12-31T21:30:30.543Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    # ── BATCH 2 (deals Jan 19–29) ──────────────────────────────────────────
    {"id":"212806680487","createdate":"2026-04-01T12:48:56.417Z","utm_campaign":"","utm_content":""},
    {"id":"198727921687","createdate":"2026-02-02T22:13:32.198Z","utm_campaign":"","utm_content":""},
    {"id":"197498598268","createdate":"2026-01-29T14:57:02.388Z","utm_campaign":"","utm_content":""},
    {"id":"197348698223","createdate":"2026-01-29T04:28:20.236Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V2] [PAID MEDIA BUYER] [02]"},
    {"id":"197283752092","createdate":"2026-01-29T01:11:32.875Z","utm_campaign":"","utm_content":""},
    {"id":"197361507936","createdate":"2026-01-28T23:56:05.030Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"197277108753","createdate":"2026-01-28T22:53:48.990Z","utm_campaign":"","utm_content":""},
    {"id":"197279039338","createdate":"2026-01-28T20:53:36.192Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"197238397347","createdate":"2026-01-28T19:13:03.175Z","utm_campaign":"","utm_content":""},
    {"id":"197247558035","createdate":"2026-01-28T18:49:13.918Z","utm_campaign":"","utm_content":""},
    {"id":"197192100442","createdate":"2026-01-28T16:52:37.854Z","utm_campaign":"","utm_content":""},
    {"id":"197150251799","createdate":"2026-01-28T14:34:10.209Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"197110021773","createdate":"2026-01-28T11:11:03.435Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"196845453723","createdate":"2026-01-28T03:16:54.830Z","utm_campaign":"","utm_content":""},
    {"id":"197012057401","createdate":"2026-01-27T23:28:27.321Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"196346603074","createdate":"2026-01-27T20:23:52.685Z","utm_campaign":"","utm_content":""},
    {"id":"196724666029","createdate":"2026-01-27T17:35:32.685Z","utm_campaign":"","utm_content":""},
    {"id":"196653651640","createdate":"2026-01-27T13:50:03.149Z","utm_campaign":"google_23273288139","utm_content":"ad_784640909659"},
    {"id":"196460076459","createdate":"2026-01-27T10:11:17.095Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"196401210800","createdate":"2026-01-27T05:10:16.300Z","utm_campaign":"","utm_content":""},
    {"id":"196398013537","createdate":"2026-01-26T23:03:56.920Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"196394496770","createdate":"2026-01-26T22:54:22.047Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"196397658471","createdate":"2026-01-26T22:54:02.688Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V3] [PAID MEDIA BUYER] [03]"},
    {"id":"196255799363","createdate":"2026-01-26T21:32:56.849Z","utm_campaign":"","utm_content":""},
    {"id":"196321180498","createdate":"2026-01-26T20:33:09.241Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"196106520955","createdate":"2026-01-26T05:34:25.525Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"196097854242","createdate":"2026-01-26T05:15:51.787Z","utm_campaign":"","utm_content":""},
    {"id":"196064445870","createdate":"2026-01-26T03:41:09.584Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V3] [PAID MEDIA BUYER] [03]"},
    {"id":"196053418074","createdate":"2026-01-26T02:35:26.707Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V3] [PAID MEDIA BUYER] [03]"},
    {"id":"196045442201","createdate":"2026-01-26T01:35:28.324Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"196031554145","createdate":"2026-01-25T23:43:32.197Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"196029634656","createdate":"2026-01-25T23:18:20.710Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"196020855695","createdate":"2026-01-25T21:45:10.238Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"195587215384","createdate":"2026-01-25T17:38:42.883Z","utm_campaign":"","utm_content":""},
    {"id":"195970985812","createdate":"2026-01-25T16:29:17.061Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"195898992282","createdate":"2026-01-25T08:16:32.637Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195877242959","createdate":"2026-01-25T08:00:54.972Z","utm_campaign":"","utm_content":""},
    {"id":"195882560896","createdate":"2026-01-25T04:29:31.127Z","utm_campaign":"","utm_content":""},
    {"id":"195816729605","createdate":"2026-01-24T18:53:16.332Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V2] [PAID MEDIA BUYER] [02]"},
    {"id":"195772602278","createdate":"2026-01-24T15:54:51.084Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195691743531","createdate":"2026-01-24T10:08:02.688Z","utm_campaign":"","utm_content":""},
    {"id":"195567166236","createdate":"2026-01-24T09:26:58.399Z","utm_campaign":"","utm_content":""},
    {"id":"195600943190","createdate":"2026-01-24T05:23:54.452Z","utm_campaign":"","utm_content":""},
    {"id":"195623225640","createdate":"2026-01-23T22:56:51.807Z","utm_campaign":"","utm_content":""},
    {"id":"195688242606","createdate":"2026-01-23T22:03:35.565Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195632632148","createdate":"2026-01-23T20:46:20.585Z","utm_campaign":"","utm_content":""},
    {"id":"195590352907","createdate":"2026-01-23T20:12:31.980Z","utm_campaign":"","utm_content":""},
    {"id":"195607779160","createdate":"2026-01-23T20:01:26.241Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195508867723","createdate":"2026-01-23T14:37:44.931Z","utm_campaign":"","utm_content":""},
    {"id":"195498249055","createdate":"2026-01-23T13:25:53.377Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195409895710","createdate":"2026-01-23T03:26:53.470Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195322861863","createdate":"2026-01-22T20:33:06.604Z","utm_campaign":"","utm_content":""},
    {"id":"195305942793","createdate":"2026-01-22T19:47:32.527Z","utm_campaign":"","utm_content":""},
    {"id":"195231992993","createdate":"2026-01-22T13:46:56.868Z","utm_campaign":"","utm_content":""},
    {"id":"195091197306","createdate":"2026-01-22T09:10:34.008Z","utm_campaign":"","utm_content":""},
    {"id":"195165407032","createdate":"2026-01-22T09:07:44.853Z","utm_campaign":"","utm_content":""},
    {"id":"195043183174","createdate":"2026-01-22T02:04:28.038Z","utm_campaign":"","utm_content":""},
    {"id":"195091370526","createdate":"2026-01-22T01:50:30.241Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"195058924721","createdate":"2026-01-21T22:34:58.178Z","utm_campaign":"","utm_content":""},
    {"id":"194962961695","createdate":"2026-01-21T14:01:50.378Z","utm_campaign":"","utm_content":""},
    {"id":"194852344351","createdate":"2026-01-21T05:26:57.961Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194822842995","createdate":"2026-01-21T01:22:11.467Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194819650864","createdate":"2026-01-21T01:17:07.918Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194789859182","createdate":"2026-01-21T00:48:52.042Z","utm_campaign":"","utm_content":""},
    {"id":"194694307972","createdate":"2026-01-20T17:36:56.991Z","utm_campaign":"","utm_content":""},
    {"id":"194647546799","createdate":"2026-01-20T14:34:43.560Z","utm_campaign":"","utm_content":""},
    {"id":"194647221777","createdate":"2026-01-20T14:15:36.035Z","utm_campaign":"","utm_content":""},
    {"id":"194540351354","createdate":"2026-01-20T13:06:42.905Z","utm_campaign":"","utm_content":""},
    {"id":"194605041845","createdate":"2026-01-20T12:28:04.279Z","utm_campaign":"","utm_content":""},
    {"id":"194576366195","createdate":"2026-01-20T07:39:38.664Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194548797030","createdate":"2026-01-20T04:36:40.135Z","utm_campaign":"","utm_content":""},
    {"id":"194480357898","createdate":"2026-01-19T21:43:51.415Z","utm_campaign":"","utm_content":""},
    {"id":"194462490120","createdate":"2026-01-19T20:53:08.166Z","utm_campaign":"","utm_content":""},
    {"id":"194329074187","createdate":"2026-01-19T20:34:31.965Z","utm_campaign":"","utm_content":""},
    {"id":"194457622547","createdate":"2026-01-19T20:24:57.478Z","utm_campaign":"","utm_content":""},
    {"id":"194447495526","createdate":"2026-01-19T19:01:23.834Z","utm_campaign":"","utm_content":""},
    {"id":"194441773867","createdate":"2026-01-19T18:15:19.590Z","utm_campaign":"","utm_content":""},
    {"id":"194434816360","createdate":"2026-01-19T16:28:25.683Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"194351357304","createdate":"2026-01-19T13:39:26.240Z","utm_campaign":"","utm_content":""},
    # ── BATCH 3 (deals Jan 29–Feb 12) ─────────────────────────────────────
    {"id":"201762790948","createdate":"2026-02-12T13:46:09.465Z","utm_campaign":"","utm_content":""},
    {"id":"201615257760","createdate":"2026-02-12T00:20:08.961Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"201583516034","createdate":"2026-02-11T20:58:48.156Z","utm_campaign":"","utm_content":""},
    {"id":"201542482312","createdate":"2026-02-11T19:57:44.802Z","utm_campaign":"headline","utm_content":""},
    {"id":"201523635794","createdate":"2026-02-11T19:10:07.165Z","utm_campaign":"","utm_content":""},
    {"id":"201524991356","createdate":"2026-02-11T17:21:17.653Z","utm_campaign":"","utm_content":""},
    {"id":"201220184851","createdate":"2026-02-11T12:14:09.414Z","utm_campaign":"","utm_content":""},
    {"id":"201209185850","createdate":"2026-02-11T10:56:03.660Z","utm_campaign":"","utm_content":""},
    {"id":"201374665015","createdate":"2026-02-11T01:24:41.682Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"201324272937","createdate":"2026-02-10T23:54:24.841Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"201094800696","createdate":"2026-02-10T21:31:33.912Z","utm_campaign":"","utm_content":""},
    {"id":"201045576452","createdate":"2026-02-10T16:08:26.656Z","utm_campaign":"","utm_content":""},
    {"id":"200992693802","createdate":"2026-02-10T13:37:25.131Z","utm_campaign":"","utm_content":""},
    {"id":"200910703618","createdate":"2026-02-10T04:06:36.858Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"200852041240","createdate":"2026-02-10T02:21:39.282Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"200874303587","createdate":"2026-02-10T01:07:50.785Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"200872532144","createdate":"2026-02-09T23:20:45.480Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"200645640619","createdate":"2026-02-09T17:32:29.930Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"200440561965","createdate":"2026-02-09T02:11:41.495Z","utm_campaign":"","utm_content":""},
    {"id":"200449560475","createdate":"2026-02-09T02:04:54.325Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"200367104147","createdate":"2026-02-08T16:50:52.046Z","utm_campaign":"","utm_content":""},
    {"id":"200362282850","createdate":"2026-02-08T14:52:41.741Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"200333892669","createdate":"2026-02-08T11:21:39.712Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"200292277534","createdate":"2026-02-08T04:56:24.519Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"200277222268","createdate":"2026-02-08T04:06:33.804Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"200234306105","createdate":"2026-02-07T20:56:57.818Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"200224419188","createdate":"2026-02-07T19:51:49.445Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"200087474345","createdate":"2026-02-06T21:41:38.757Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"200086505647","createdate":"2026-02-06T21:24:25.167Z","utm_campaign":"","utm_content":""},
    {"id":"200080648761","createdate":"2026-02-06T20:00:57.952Z","utm_campaign":"","utm_content":""},
    {"id":"200022623921","createdate":"2026-02-06T18:03:22.736Z","utm_campaign":"","utm_content":""},
    {"id":"199971603558","createdate":"2026-02-06T12:45:07.895Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"199883502414","createdate":"2026-02-06T01:50:59Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORM%5D+%5B01%5D","utm_content":""},
    {"id":"199864913429","createdate":"2026-02-06T01:22:46.873Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"199792217971","createdate":"2026-02-05T18:37:52.075Z","utm_campaign":"","utm_content":""},
    {"id":"199729767067","createdate":"2026-02-05T14:11:18.277Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"199710188977","createdate":"2026-02-05T12:50:48.041Z","utm_campaign":"","utm_content":""},
    {"id":"199695785804","createdate":"2026-02-05T12:18:00.793Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"199571463583","createdate":"2026-02-05T00:12:13.219Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORM%5D+%5B01%5D","utm_content":""},
    {"id":"199555613981","createdate":"2026-02-05T00:02:14.132Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"199539864755","createdate":"2026-02-04T23:39:07.186Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"199510403760","createdate":"2026-02-04T20:57:14.365Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"199481706346","createdate":"2026-02-04T19:09:18.197Z","utm_campaign":"","utm_content":""},
    {"id":"199472660494","createdate":"2026-02-04T17:25:04.581Z","utm_campaign":"","utm_content":""},
    {"id":"199440758806","createdate":"2026-02-04T16:48:58.358Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORM%5D+%5B01%5D","utm_content":""},
    {"id":"199422495769","createdate":"2026-02-04T13:59:46.301Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"199413031842","createdate":"2026-02-04T12:54:09.684Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"195815255688","createdate":"2026-02-04T11:15:56.993Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"199341542057","createdate":"2026-02-04T08:16:36.025Z","utm_campaign":"","utm_content":"link_in_bio"},
    {"id":"199333886334","createdate":"2026-02-04T05:27:37.413Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"199297942343","createdate":"2026-02-04T04:26:48.458Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"197611804067","createdate":"2026-02-04T02:54:16.478Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V3] [PAID MEDIA BUYER] [03]"},
    {"id":"198881202033","createdate":"2026-02-02T23:52:19.794Z","utm_campaign":"","utm_content":""},
    {"id":"198848839086","createdate":"2026-02-02T23:44:58.628Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"198837359876","createdate":"2026-02-02T21:20:26.245Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198836333615","createdate":"2026-02-02T21:05:23.080Z","utm_campaign":"","utm_content":""},
    {"id":"198813644433","createdate":"2026-02-02T20:56:50.197Z","utm_campaign":"","utm_content":""},
    {"id":"198832067375","createdate":"2026-02-02T20:48:31.817Z","utm_campaign":"","utm_content":""},
    {"id":"198716843285","createdate":"2026-02-02T16:00:33.893Z","utm_campaign":"","utm_content":""},
    {"id":"198715336207","createdate":"2026-02-02T14:19:43.580Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198669820821","createdate":"2026-02-02T11:54:57.768Z","utm_campaign":"","utm_content":""},
    {"id":"198561137465","createdate":"2026-02-02T05:41:48.911Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198561552806","createdate":"2026-02-02T05:29:27.470Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198373071206","createdate":"2026-02-01T14:11:09.709Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"198320345769","createdate":"2026-02-01T10:32:46.808Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198315991635","createdate":"2026-02-01T08:49:18.808Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198292528517","createdate":"2026-02-01T07:06:34.953Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198264929617","createdate":"2026-02-01T02:47:35.623Z","utm_campaign":"","utm_content":""},
    {"id":"198169659743","createdate":"2026-01-31T13:34:16.014Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"198155148050","createdate":"2026-01-31T13:02:29.673Z","utm_campaign":"","utm_content":""},
    {"id":"193581456824","createdate":"2026-01-30T10:57:32.825Z","utm_campaign":"[MARKETING] [SCHEDULES] [66] [NEW TEST]","utm_content":"[V3] [PAID MEDIA BUYER] [03]"},
    {"id":"197765547673","createdate":"2026-01-30T09:51:33.722Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"197581275701","createdate":"2026-01-29T18:40:15.989Z","utm_campaign":"","utm_content":""},
    {"id":"197504263567","createdate":"2026-01-29T15:49:47.886Z","utm_campaign":"","utm_content":""},
    {"id":"197475903254","createdate":"2026-01-29T13:27:50.346Z","utm_campaign":"","utm_content":""},
    {"id":"197483796480","createdate":"2026-01-29T13:27:50.328Z","utm_campaign":"","utm_content":""},
    {"id":"197337963787","createdate":"2026-01-29T01:59:43.819Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    # ── BATCH 4 (deals Feb 12–Mar 05) ─────────────────────────────────────
    {"id":"213519861504","createdate":"2026-04-06T11:44:06.598Z","utm_campaign":"","utm_content":""},
    {"id":"207134985996","createdate":"2026-03-05T03:25:38.331Z","utm_campaign":"","utm_content":""},
    {"id":"207088721748","createdate":"2026-03-04T20:33:59.756Z","utm_campaign":"","utm_content":""},
    {"id":"206986367769","createdate":"2026-03-04T13:57:47.555Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"206832952846","createdate":"2026-03-03T20:19:42.835Z","utm_campaign":"","utm_content":""},
    {"id":"206718812983","createdate":"2026-03-03T15:12:18.890Z","utm_campaign":"","utm_content":"link_in_bio"},
    {"id":"206700217945","createdate":"2026-03-03T13:49:14.754Z","utm_campaign":"","utm_content":""},
    {"id":"206571712615","createdate":"2026-03-03T05:18:01.870Z","utm_campaign":"[MARKETING] [SCHEDULES] [69] [SHORT GHL LP]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"206284420790","createdate":"2026-03-01T23:29:59.079Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"206238350337","createdate":"2026-03-01T17:28:17.496Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"206225306640","createdate":"2026-03-01T15:43:52.351Z","utm_campaign":"[MARKETING] [SCHEDULES] [69] [SHORT GHL LP]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"206221566720","createdate":"2026-03-01T14:48:49.531Z","utm_campaign":"","utm_content":""},
    {"id":"206190234165","createdate":"2026-03-01T12:20:05.759Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"206172596766","createdate":"2026-03-01T09:57:03.093Z","utm_campaign":"","utm_content":""},
    {"id":"206114947423","createdate":"2026-02-28T22:55:09.720Z","utm_campaign":"","utm_content":""},
    {"id":"206025481263","createdate":"2026-02-28T11:39:11.464Z","utm_campaign":"[MARKETING] [SCHEDULES] [69] [SHORT GHL LP]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"205954331489","createdate":"2026-02-28T01:56:07.528Z","utm_campaign":"","utm_content":""},
    {"id":"205717328748","createdate":"2026-02-26T23:49:04.462Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"205665557132","createdate":"2026-02-26T20:32:31.825Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"205634616604","createdate":"2026-02-26T18:54:21.998Z","utm_campaign":"","utm_content":""},
    {"id":"205614587476","createdate":"2026-02-26T17:04:10.509Z","utm_campaign":"","utm_content":""},
    {"id":"205612116571","createdate":"2026-02-26T16:36:22.693Z","utm_campaign":"","utm_content":""},
    {"id":"205591142920","createdate":"2026-02-26T15:34:34.742Z","utm_campaign":"","utm_content":""},
    {"id":"205196774270","createdate":"2026-02-26T05:59:02.130Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"205403643966","createdate":"2026-02-25T20:12:09.147Z","utm_campaign":"","utm_content":""},
    {"id":"205341472393","createdate":"2026-02-25T16:21:29.600Z","utm_campaign":"","utm_content":""},
    {"id":"205301642810","createdate":"2026-02-25T14:11:21.984Z","utm_campaign":"","utm_content":""},
    {"id":"205198510990","createdate":"2026-02-25T03:38:46.808Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"205097056900","createdate":"2026-02-24T15:54:27.192Z","utm_campaign":"","utm_content":""},
    {"id":"205056621359","createdate":"2026-02-24T13:09:58.974Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"204943019777","createdate":"2026-02-24T03:21:08.614Z","utm_campaign":"","utm_content":""},
    {"id":"201906701488","createdate":"2026-02-24T00:52:29.328Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"204811914259","createdate":"2026-02-23T21:12:40.406Z","utm_campaign":"","utm_content":""},
    {"id":"204741903125","createdate":"2026-02-23T13:35:30.314Z","utm_campaign":"","utm_content":""},
    {"id":"204720392327","createdate":"2026-02-23T11:13:12.352Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"204613769891","createdate":"2026-02-23T01:04:57.619Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"204588568863","createdate":"2026-02-22T21:21:29.738Z","utm_campaign":"","utm_content":"link_in_bio"},
    {"id":"204585110193","createdate":"2026-02-22T20:56:51.420Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"204442413943","createdate":"2026-02-21T22:21:08.650Z","utm_campaign":"","utm_content":""},
    {"id":"204205125464","createdate":"2026-02-20T21:34:57.557Z","utm_campaign":"","utm_content":""},
    {"id":"204202769839","createdate":"2026-02-20T17:14:27.846Z","utm_campaign":"","utm_content":""},
    {"id":"204114372936","createdate":"2026-02-20T14:14:38.538Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"204022316664","createdate":"2026-02-20T04:13:36.107Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"203912973422","createdate":"2026-02-19T17:42:43.476Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"203669294000","createdate":"2026-02-19T02:18:30.809Z","utm_campaign":"","utm_content":""},
    {"id":"203697973634","createdate":"2026-02-18T21:16:33.748Z","utm_campaign":"","utm_content":"link_in_bio"},
    {"id":"203677585303","createdate":"2026-02-18T19:51:01.632Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"203659829258","createdate":"2026-02-18T18:18:35.092Z","utm_campaign":"","utm_content":""},
    {"id":"203625331633","createdate":"2026-02-18T14:51:13.042Z","utm_campaign":"","utm_content":""},
    {"id":"203570974809","createdate":"2026-02-18T12:34:09.649Z","utm_campaign":"","utm_content":""},
    {"id":"203495347249","createdate":"2026-02-18T05:37:07.676Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"203018757712","createdate":"2026-02-16T19:52:15.117Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"203014038534","createdate":"2026-02-16T18:55:28.176Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"202891692619","createdate":"2026-02-16T11:40:01.704Z","utm_campaign":"","utm_content":""},
    {"id":"202717480818","createdate":"2026-02-16T01:20:33.974Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"202686223219","createdate":"2026-02-15T21:27:59.497Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"202609049930","createdate":"2026-02-15T11:25:13.049Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"202606628166","createdate":"2026-02-15T10:45:50.155Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"202562566008","createdate":"2026-02-15T05:55:05.431Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"202447177347","createdate":"2026-02-14T20:19:16.015Z","utm_campaign":"[MARKETING] [SCHEDULES] [62]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"202416283710","createdate":"2026-02-14T15:09:24.601Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"202354637909","createdate":"2026-02-14T09:56:48.979Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"202250417843","createdate":"2026-02-13T21:51:38.390Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"202040391216","createdate":"2026-02-13T14:06:42.392Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"202037927493","createdate":"2026-02-13T13:15:25.005Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"201965945198","createdate":"2026-02-13T08:30:35.547Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"201931735870","createdate":"2026-02-13T02:58:01.540Z","utm_campaign":"[MARKETING] [SCHEDULES] [57]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"201778961948","createdate":"2026-02-12T14:36:35.281Z","utm_campaign":"","utm_content":""},
    {"id":"201228254815","createdate":"2026-02-11T13:49:37.155Z","utm_campaign":"","utm_content":""},
    # ── BATCH 5 (deals Mar 05–24) ──────────────────────────────────────────
    {"id":"214210450999","createdate":"2026-04-08T12:19:40.749Z","utm_campaign":"","utm_content":""},
    {"id":"211367083016","createdate":"2026-03-24T21:25:18.767Z","utm_campaign":"","utm_content":""},
    {"id":"211346809159","createdate":"2026-03-24T21:21:56.141Z","utm_campaign":"","utm_content":""},
    {"id":"211351129716","createdate":"2026-03-24T21:20:41.517Z","utm_campaign":"","utm_content":""},
    {"id":"211344409945","createdate":"2026-03-24T18:01:57.993Z","utm_campaign":"","utm_content":""},
    {"id":"211367658117","createdate":"2026-03-24T18:00:46.695Z","utm_campaign":"","utm_content":""},
    {"id":"211122418613","createdate":"2026-03-24T13:49:52.236Z","utm_campaign":"","utm_content":""},
    {"id":"211273362448","createdate":"2026-03-24T13:27:26.608Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"211269268895","createdate":"2026-03-24T12:33:01.789Z","utm_campaign":"","utm_content":""},
    {"id":"211250702640","createdate":"2026-03-24T11:38:00.328Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"211148795207","createdate":"2026-03-24T00:10:29.549Z","utm_campaign":"[MARKETING] [SCHEDULES] [64]","utm_content":"[JERRICA] [SOCIAL MEDIA] [01]"},
    {"id":"211016184188","createdate":"2026-03-23T13:01:11.508Z","utm_campaign":"","utm_content":""},
    {"id":"210883702692","createdate":"2026-03-22T19:20:39.105Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"210750372199","createdate":"2026-03-21T18:52:52.152Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - scalearmy.com/social-media-manager"},
    {"id":"192042678684","createdate":"2026-03-21T03:31:27.022Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"210597955507","createdate":"2026-03-20T16:01:45.189Z","utm_campaign":"","utm_content":""},
    {"id":"210405487245","createdate":"2026-03-19T20:33:54.684Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"210341222149","createdate":"2026-03-19T14:44:57.428Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - scalearmy.com/social-media-manager"},
    {"id":"210198851460","createdate":"2026-03-18T21:01:35.409Z","utm_campaign":"","utm_content":""},
    {"id":"209973556777","createdate":"2026-03-18T13:31:01.177Z","utm_campaign":"","utm_content":""},
    {"id":"209934904694","createdate":"2026-03-18T00:43:01.997Z","utm_campaign":"","utm_content":""},
    {"id":"209918410618","createdate":"2026-03-17T21:17:11.598Z","utm_campaign":"","utm_content":""},
    {"id":"209863053333","createdate":"2026-03-17T16:39:08.262Z","utm_campaign":"","utm_content":""},
    {"id":"209688025871","createdate":"2026-03-17T04:43:13.115Z","utm_campaign":"[MARKETING] [LEADS] [80]","utm_content":"Jerrica - #02 - scalearmy.com/social-media-manager"},
    {"id":"209663036715","createdate":"2026-03-17T01:03:21.061Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"209585212222","createdate":"2026-03-16T17:57:58.777Z","utm_campaign":"","utm_content":""},
    {"id":"209507789863","createdate":"2026-03-16T13:20:57.222Z","utm_campaign":"","utm_content":""},
    {"id":"209397888064","createdate":"2026-03-15T22:25:26.205Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"209197828748","createdate":"2026-03-14T14:38:17.893Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[AI] [SOCIAL MEDIA] [03]"},
    {"id":"209173555319","createdate":"2026-03-14T11:30:22.796Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"209041570443","createdate":"2026-03-13T18:21:09.052Z","utm_campaign":"","utm_content":""},
    {"id":"208936505270","createdate":"2026-03-13T04:18:22.625Z","utm_campaign":"","utm_content":""},
    {"id":"208933320098","createdate":"2026-03-13T02:29:07.495Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"208895609516","createdate":"2026-03-12T22:50:20.247Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"208892749492","createdate":"2026-03-12T22:15:28.800Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"208810157873","createdate":"2026-03-12T14:48:55.491Z","utm_campaign":"[MARKETING] [SCHEDULES] [67]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"208693438211","createdate":"2026-03-12T04:37:32Z","utm_campaign":"%5BMARKETING%5D+%5BSCHEDULES%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"208620144803","createdate":"2026-03-12T04:26:42.410Z","utm_campaign":"%5BMARKETING%5D+%5BSCHEDULES%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"208550142077","createdate":"2026-03-11T13:34:57.152Z","utm_campaign":"","utm_content":""},
    {"id":"208415184754","createdate":"2026-03-10T19:01:10.337Z","utm_campaign":"","utm_content":""},
    {"id":"208286929077","createdate":"2026-03-10T11:40:09.293Z","utm_campaign":"[MARKETING] [SCHEDULES] [75] [MARKETING-TEAMS]","utm_content":"Jerrica Social Media - #02"},
    {"id":"208223195659","createdate":"2026-03-10T11:03:10.070Z","utm_campaign":"","utm_content":""},
    {"id":"204468373915","createdate":"2026-03-09T02:18:19.217Z","utm_campaign":"[MARKETING] [SCHEDULES] [73] [MARKETING-TEAMS]","utm_content":"AI Social Media Manager - #01"},
    {"id":"207937150039","createdate":"2026-03-09T00:50:43.208Z","utm_campaign":"[MARKETING] [SCHEDULES] [71] [MARKETING-TEAMS]","utm_content":"Content Marketing Manager - #02"},
    {"id":"207836957209","createdate":"2026-03-08T10:15:42.626Z","utm_campaign":"[MARKETING] [SCHEDULES] [71] [MARKETING-TEAMS]","utm_content":"AI Social Media - #03"},
    {"id":"207794476175","createdate":"2026-03-08T03:52:41.852Z","utm_campaign":"[MARKETING] [SCHEDULES] [71] [MARKETING-TEAMS]","utm_content":"AI Social Media - #03"},
    {"id":"207717791042","createdate":"2026-03-07T15:12:55.539Z","utm_campaign":"%5BMARKETING%5D+%5BSCHEDULES%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"207681877149","createdate":"2026-03-07T10:51:30.032Z","utm_campaign":"%5BMARKETING%5D+%5BSCHEDULES%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"207468980265","createdate":"2026-03-06T13:20:40.286Z","utm_campaign":"120240227787380177","utm_content":"120240227787410177"},
    {"id":"207402366977","createdate":"2026-03-06T05:05:28.299Z","utm_campaign":"[MARKETING] [SCHEDULES] [74] [MARKETING-TEAMS]","utm_content":"AI Content Marketing Manager - #03"},
    {"id":"207396610649","createdate":"2026-03-06T03:27:35.980Z","utm_campaign":"%5BMARKETING%5D+%5BSCHEDULES%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"207376631222","createdate":"2026-03-06T02:14:34.156Z","utm_campaign":"[MARKETING] [SCHEDULES] [75] [MARKETING-TEAMS]","utm_content":"Jerrica Social Media - #02"},
    {"id":"207300122128","createdate":"2026-03-05T18:56:51.132Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    {"id":"207159658351","createdate":"2026-03-05T05:21:09.978Z","utm_campaign":"[MARKETING] [SCHEDULES] [68] [MARKETING-TEAMS]","utm_content":"[JERRICA] [SOCIAL MEDIA] [02]"},
    # ── BATCH 6 (deals Mar 24–Apr 04) ─────────────────────────────────────
    {"id":"213683027883","createdate":"2026-04-04T19:38:26.691Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"213642920345","createdate":"2026-04-04T14:52:50.839Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - scalearmy.com/social-media-manager"},
    {"id":"213610871934","createdate":"2026-04-04T05:10:49.466Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"213578625905","createdate":"2026-04-03T21:53:08.397Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    {"id":"213278632359","createdate":"2026-04-02T17:56:34.266Z","utm_campaign":"","utm_content":"link_in_bio"},
    {"id":"213261874349","createdate":"2026-04-02T16:24:46.137Z","utm_campaign":"","utm_content":""},
    {"id":"213218320459","createdate":"2026-04-02T13:52:27.146Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"196198294558","createdate":"2026-04-02T13:35:26.387Z","utm_campaign":"{{campaign.name}}","utm_content":"{{ad.name}}"},
    {"id":"213201045176","createdate":"2026-04-02T13:05:14.232Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"213193520905","createdate":"2026-04-02T12:24:12.111Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"212998322064","createdate":"2026-04-01T13:07:00.014Z","utm_campaign":"","utm_content":""},
    {"id":"212962054000","createdate":"2026-04-01T11:30:38.247Z","utm_campaign":"","utm_content":""},
    {"id":"212915340414","createdate":"2026-04-01T06:54:34.934Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"212787032611","createdate":"2026-03-31T17:52:08.924Z","utm_campaign":"","utm_content":""},
    {"id":"212835375380","createdate":"2026-03-31T17:06:27.092Z","utm_campaign":"","utm_content":""},
    {"id":"212831365195","createdate":"2026-03-31T16:07:46.790Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"212610041510","createdate":"2026-03-31T07:08:29.983Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"212602606858","createdate":"2026-03-31T02:24:43.804Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"212460141364","createdate":"2026-03-30T10:15:57.195Z","utm_campaign":"","utm_content":""},
    {"id":"212462972025","createdate":"2026-03-30T10:15:56.207Z","utm_campaign":"","utm_content":""},
    {"id":"212080249160","createdate":"2026-03-30T00:13:38.296Z","utm_campaign":"[MARKETING] [SCHEDULES] [76] [MARKETING-TEAMS]","utm_content":"[GENERIC 2 - JANUARY 26]"},
    {"id":"209222055178","createdate":"2026-03-29T21:17:09.927Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"212377882663","createdate":"2026-03-29T20:49:56.585Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"190073427126","createdate":"2026-03-29T14:25:40.743Z","utm_campaign":"[MARKETING] [SCHEDULES] [11]","utm_content":"[CMM-V2] [Content Marketing Manager] [01]"},
    {"id":"212254758695","createdate":"2026-03-28T23:06:21.270Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"212055794701","createdate":"2026-03-28T00:56:31.076Z","utm_campaign":"%5BMARKETING%5D+%5BINSTANT+FORMS%5D+%5B76%5D+%5BMARKETING-TEAMS%5D","utm_content":""},
    {"id":"212059635742","createdate":"2026-03-27T21:19:50.254Z","utm_campaign":"","utm_content":""},
    {"id":"212076985966","createdate":"2026-03-27T17:17:46.115Z","utm_campaign":"","utm_content":""},
    {"id":"211887797156","createdate":"2026-03-27T00:36:42.607Z","utm_campaign":"[MARKETING] [LEADS] [80]","utm_content":"Jerrica - #02 - scalearmy.com/social-media-manager"},
    {"id":"211835146127","createdate":"2026-03-26T18:14:01.341Z","utm_campaign":"","utm_content":""},
    {"id":"211835299862","createdate":"2026-03-26T18:10:24.644Z","utm_campaign":"","utm_content":""},
    {"id":"211799960737","createdate":"2026-03-26T17:48:16.165Z","utm_campaign":"","utm_content":""},
    {"id":"211801945497","createdate":"2026-03-26T17:44:16.066Z","utm_campaign":"","utm_content":""},
    {"id":"211807168416","createdate":"2026-03-26T17:40:11.903Z","utm_campaign":"","utm_content":""},
    {"id":"211809056386","createdate":"2026-03-26T17:27:00.542Z","utm_campaign":"","utm_content":""},
    {"id":"211800109580","createdate":"2026-03-26T17:26:46.205Z","utm_campaign":"","utm_content":""},
    {"id":"211771162473","createdate":"2026-03-26T16:03:34.866Z","utm_campaign":"","utm_content":""},
    {"id":"211775865441","createdate":"2026-03-26T15:29:26.513Z","utm_campaign":"","utm_content":""},
    {"id":"211765243043","createdate":"2026-03-26T15:19:32.566Z","utm_campaign":"","utm_content":""},
    {"id":"211760011122","createdate":"2026-03-26T15:08:41.213Z","utm_campaign":"","utm_content":""},
    {"id":"211612183941","createdate":"2026-03-26T01:15:40.691Z","utm_campaign":"","utm_content":""},
    {"id":"211605214488","createdate":"2026-03-26T00:33:08.076Z","utm_campaign":"","utm_content":""},
    {"id":"211612811897","createdate":"2026-03-26T00:30:49.070Z","utm_campaign":"","utm_content":""},
    {"id":"211600428875","createdate":"2026-03-26T00:25:48.996Z","utm_campaign":"","utm_content":""},
    {"id":"211595639889","createdate":"2026-03-26T00:02:11.734Z","utm_campaign":"","utm_content":""},
    {"id":"211614024315","createdate":"2026-03-25T23:53:49.757Z","utm_campaign":"","utm_content":""},
    {"id":"211614193581","createdate":"2026-03-25T23:51:07.013Z","utm_campaign":"","utm_content":""},
    {"id":"211615421786","createdate":"2026-03-25T23:49:51.264Z","utm_campaign":"","utm_content":""},
    {"id":"211599795481","createdate":"2026-03-25T23:29:22.804Z","utm_campaign":"","utm_content":""},
    {"id":"211623567730","createdate":"2026-03-25T20:46:14.897Z","utm_campaign":"","utm_content":""},
    {"id":"211592462511","createdate":"2026-03-25T20:11:58.997Z","utm_campaign":"","utm_content":""},
    {"id":"211600691091","createdate":"2026-03-25T20:01:33.790Z","utm_campaign":"","utm_content":""},
    {"id":"211613053049","createdate":"2026-03-25T19:51:25.875Z","utm_campaign":"","utm_content":""},
    {"id":"211602069543","createdate":"2026-03-25T19:29:48.806Z","utm_campaign":"","utm_content":""},
    {"id":"211598605894","createdate":"2026-03-25T19:29:14.776Z","utm_campaign":"","utm_content":""},
    {"id":"211601293849","createdate":"2026-03-25T19:16:57.935Z","utm_campaign":"","utm_content":""},
    {"id":"211602376247","createdate":"2026-03-25T19:16:06.666Z","utm_campaign":"","utm_content":""},
    {"id":"211614438930","createdate":"2026-03-25T19:15:51.353Z","utm_campaign":"","utm_content":""},
    {"id":"211560391192","createdate":"2026-03-25T18:03:16.045Z","utm_campaign":"","utm_content":""},
    {"id":"211553310628","createdate":"2026-03-25T17:39:58.782Z","utm_campaign":"","utm_content":""},
    {"id":"211551252276","createdate":"2026-03-25T16:57:01.128Z","utm_campaign":"","utm_content":""},
    {"id":"211555970928","createdate":"2026-03-25T16:50:21.265Z","utm_campaign":"","utm_content":""},
    {"id":"211388203796","createdate":"2026-03-25T14:18:31.249Z","utm_campaign":"","utm_content":""},
    {"id":"211340488204","createdate":"2026-03-25T14:17:56.989Z","utm_campaign":"","utm_content":""},
    {"id":"211340553603","createdate":"2026-03-25T14:16:54.026Z","utm_campaign":"","utm_content":""},
    {"id":"211340554779","createdate":"2026-03-25T14:16:01.176Z","utm_campaign":"","utm_content":""},
    {"id":"211388203444","createdate":"2026-03-25T14:15:16.311Z","utm_campaign":"","utm_content":""},
    {"id":"211492683937","createdate":"2026-03-25T12:28:05.511Z","utm_campaign":"","utm_content":""},
    {"id":"211340287665","createdate":"2026-03-25T10:05:26.932Z","utm_campaign":"[MARKETING] [SCHEDULES] [76] [MARKETING-TEAMS]","utm_content":"[GENERIC 2 - JANUARY 26]"},
    {"id":"211175492652","createdate":"2026-03-24T23:44:22.470Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"211372977297","createdate":"2026-03-24T21:41:48.307Z","utm_campaign":"","utm_content":""},
    # ── BATCH 7 (deals Apr 04–15) ──────────────────────────────────────────
    {"id":"215716264103","createdate":"2026-04-15T12:25:15.773Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Generic Nate - #01"},
    {"id":"215723507024","createdate":"2026-04-15T12:24:13.386Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Generic Nate - #01"},
    {"id":"215686919337","createdate":"2026-04-15T06:52:38.463Z","utm_campaign":"82_Leads_Landing_Page_Schedule_Event_AI_Automation_Specialist","utm_content":"AD135_VID_Founder_friend_told_me"},
    {"id":"215643547449","createdate":"2026-04-14T23:49:28.277Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"215641237282","createdate":"2026-04-14T23:19:43.710Z","utm_campaign":"82_Leads_Landing_Page_Schedule_Event_AI_Automation_Specialist","utm_content":"AD133_VID_Friend_of_my_told_me"},
    {"id":"215595621747","createdate":"2026-04-14T18:41:44.640Z","utm_campaign":"","utm_content":""},
    {"id":"215550063493","createdate":"2026-04-14T13:33:09.697Z","utm_campaign":"81_Leads_Landing_Page_Schedule_Event_Marketing_Teams","utm_content":"AD01_IMG_Wasting_money_Ops_managers — Cópia"},
    {"id":"215469915545","createdate":"2026-04-14T02:11:01.969Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"215420461729","createdate":"2026-04-13T19:58:04.818Z","utm_campaign":"76_Leads_Instant_Form_Lead_Event_Marketing_Teams","utm_content":"AD02_VID_Jerrica_80k_year_SMM"},
    {"id":"215363323141","createdate":"2026-04-13T14:31:59.210Z","utm_campaign":"","utm_content":""},
    {"id":"215353068053","createdate":"2026-04-13T14:25:17.162Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"215355723371","createdate":"2026-04-13T14:07:15.478Z","utm_campaign":"81_Leads_Landing_Page_Schedule_Event_Marketing_Teams","utm_content":"AD01_IMG_Wasting_money_Ops_managers — Cópia"},
    {"id":"215188715954","createdate":"2026-04-12T14:35:11.978Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"215188458023","createdate":"2026-04-12T14:34:28.053Z","utm_campaign":"76_Leads_Instant_Form_Lead_Event_Marketing_Teams","utm_content":""},
    {"id":"215169894715","createdate":"2026-04-12T10:25:11.906Z","utm_campaign":"81_Leads_Landing_Page_Schedule_Event_Marketing_Teams","utm_content":"AD01_IMG_Wasting_money_Ops_managers — Cópia"},
    {"id":"215147972431","createdate":"2026-04-12T06:56:39Z","utm_campaign":"76_Leads_Instant_Form_Lead_Event_Marketing_Teams","utm_content":""},
    {"id":"215108903859","createdate":"2026-04-11T23:02:18.132Z","utm_campaign":"81_Leads_Landing_Page_Schedule_Event_Marketing_Teams","utm_content":"AD01_IMG_Wasting_money_Ops_managers"},
    {"id":"215109056643","createdate":"2026-04-11T22:53:50.997Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Generic Nate - #01"},
    {"id":"214949098588","createdate":"2026-04-10T21:01:18.322Z","utm_campaign":"","utm_content":""},
    {"id":"214923752031","createdate":"2026-04-10T17:31:34.194Z","utm_campaign":"","utm_content":""},
    {"id":"214881139548","createdate":"2026-04-10T12:17:26.564Z","utm_campaign":"","utm_content":""},
    {"id":"214881053288","createdate":"2026-04-10T12:15:54.847Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Generic Nate - #01"},
    {"id":"214720716720","createdate":"2026-04-09T22:31:08.569Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"214689287256","createdate":"2026-04-09T16:38:03.403Z","utm_campaign":"","utm_content":""},
    {"id":"214438451264","createdate":"2026-04-09T07:32:23.208Z","utm_campaign":"[MARKETING] [SCHEDULES] [76] [MARKETING-TEAMS]","utm_content":"[GENERIC 2 - JANUARY 26]"},
    {"id":"214502821127","createdate":"2026-04-09T00:24:40.817Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    {"id":"214262970257","createdate":"2026-04-08T08:10:55.717Z","utm_campaign":"76_Leads_Instant_Form_Lead_Event_Marketing_Teams","utm_content":""},
    {"id":"214294408630","createdate":"2026-04-08T05:55:35.462Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"OM - #05 - Copy"},
    {"id":"214185725784","createdate":"2026-04-07T19:41:29.804Z","utm_campaign":"76_Leads_Instant_Form_Lead_Event_Marketing_Teams","utm_content":""},
    {"id":"213904104732","createdate":"2026-04-06T11:50:35.499Z","utm_campaign":"","utm_content":""},
    {"id":"213895991041","createdate":"2026-04-06T11:01:34.702Z","utm_campaign":"","utm_content":""},
    {"id":"213855023935","createdate":"2026-04-06T04:54:35.244Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Generic Nate - #01"},
    {"id":"213815095704","createdate":"2026-04-05T20:04:52.676Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
    {"id":"213712947515","createdate":"2026-04-05T02:22:50.721Z","utm_campaign":"76_Leads_Instant_Form_Lead_Event_Marketing_Teams","utm_content":""},
    {"id":"213684072319","createdate":"2026-04-04T20:15:01.658Z","utm_campaign":"[MARKETING] [SCHEDULES] [79]","utm_content":"Jerrica - #02 - https://scalearmy.com/hire-form-smm/"},
]

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
# Parse ISO 8601 dates → timestamp
# ---------------------------------------------------------------------------
def ts(s: str) -> float:
    s = s.rstrip("Z").split(".")[0]
    return datetime.datetime.fromisoformat(s).replace(
        tzinfo=datetime.timezone.utc
    ).timestamp()

# ---------------------------------------------------------------------------
# Build contact lookup: dedupe, sort by createdate asc
# ---------------------------------------------------------------------------
seen_ids: set = set()
CONTACTS: list = []
for c in RAW_CONTACTS:
    if c["id"] in seen_ids:
        continue
    seen_ids.add(c["id"])
    CONTACTS.append({
        "id":           c["id"],
        "ts":           ts(c["createdate"]),
        "utm_campaign": clean_utm(c.get("utm_campaign", "")),
        "utm_content":  clean_utm(c.get("utm_content", "")),
    })
CONTACTS.sort(key=lambda x: x["ts"])

print(f"Contacts loaded: {len(CONTACTS)}")

# ---------------------------------------------------------------------------
# Load deals
# ---------------------------------------------------------------------------
with open(BASE / "hs_deals_raw.json") as f:
    raw_deals: dict = json.load(f)

# tolerance for proximity matching: 2 days in seconds
TOLERANCE_SEC = 2 * 86400

def find_utm(deal_ts: float):
    """Find the closest contact by createdate within TOLERANCE_SEC."""
    best = None
    best_diff = float("inf")
    for c in CONTACTS:
        diff = abs(c["ts"] - deal_ts)
        if diff < best_diff:
            best_diff = diff
            best = c
        # since sorted, once we're far past the deal date, we can break
        if c["ts"] > deal_ts + TOLERANCE_SEC:
            break
    if best and best_diff <= TOLERANCE_SEC:
        return best["utm_campaign"], best["utm_content"]
    return "", ""

# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------
def empty_row():
    return {"booked": 0, "held": 0, "no_show": 0, "scheduled": 0}

summary   = empty_row()
by_camp: dict = {}
by_cont: dict = {}

for deal_id, info in raw_deals.items():
    stage    = info["stage"]
    deal_ts  = ts(info["createdate"])
    bucket   = classify(stage)

    summary["booked"] += 1
    summary[bucket]   += 1

    utm_c, utm_k = find_utm(deal_ts)
    utm_c = utm_c or "(no utm_campaign)"
    utm_k = utm_k or "(no utm_content)"

    for key, store in [(utm_c, by_camp), (utm_k, by_cont)]:
        if key not in store:
            store[key] = {"label": key, **empty_row()}
        store[key]["booked"] += 1
        store[key][bucket]   += 1

def add_rate(rows):
    out = []
    for r in rows.values():
        b = r["booked"]
        r["show_rate"] = round(r["held"] / b * 100, 1) if b else None
        out.append(r)
    return sorted(out, key=lambda x: -x["booked"])

b = summary["booked"]
summary["show_rate"] = round(summary["held"] / b * 100, 1) if b else None

cache = {
    "since":       "2026-01-01",
    "until":       "2026-04-15",
    "summary":     summary,
    "by_campaign": add_rate(by_camp),
    "by_content":  add_rate(by_cont),
    "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
}

out_path = BASE / "hubspot_cache.json"
with open(out_path, "w") as f:
    json.dump(cache, f, indent=2)

print(f"Cache written → {out_path}")
print(f"Summary: {summary}")
print(f"Campaigns: {len(by_camp)}, Creatives: {len(by_cont)}")
