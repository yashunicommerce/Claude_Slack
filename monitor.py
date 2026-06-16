import requests
import json
import os
from datetime import datetime, timedelta, timezone

# ── Config (injected via GitHub Secrets) ──────────────────────
SLACK_WEBHOOK_URL  = os.environ["SLACK_WEBHOOK_URL"]
GRAFANA_SESSION    = os.environ["GRAFANA_SESSION"]
ES_URL             = "https://uniwatch.unicommerce.com/db/api/datasources/proxy/114/_msearch?max_concurrent_shard_requests=100"

FAILURE_RATE_THRESHOLD = 30    # %
P90_THRESHOLD          = 5000  # ms

# ── Time range: yesterday 00:00 → today 00:00 IST ─────────────
IST = timezone(timedelta(hours=5, minutes=30))
now         = datetime.now(IST)
today       = now.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday   = today - timedelta(days=1)
gte         = int(yesterday.timestamp() * 1000)
lte         = int(today.timestamp() * 1000)
date_str    = yesterday.strftime("%Y-%m-%d")

HEADERS = {
    "accept":       "application/json, text/plain, */*",
    "content-type": "application/x-ndjson",
    "cookie":       f"grafana_session={GRAFANA_SESSION}",
    "origin":       "https://uniwatch.unicommerce.com",
    "referer":      "https://uniwatch.unicommerce.com/db/d/NoQi9W2SK/invoice_data_performance_peak",
    "user-agent":   "Mozilla/5.0",
    "x-grafana-org-id": "1"
}

INVOICE_URLS = (
    'base_url:"/data/oms/invoice/create" OR '
    'base_url:"/services/rest/v1/oms/shippingPackage/createInvoiceAndAllocateShippingProvider" OR '
    'base_url:"/services/rest/v1/oms/shippingPackage/createInvoiceAndGenerateLabel/"'
)

ALLOCATE_URLS = (
    'base_url:"/services/rest/v1/oms/shippingPackage/allocateShippingProvider" OR '
    'base_url:"/data/oms/shipment/bulk/provider/allocate" OR '
    'base_url:"/services/rest/v1/oms/shippingPackage/createInvoiceAndAllocateShippingProvider" OR '
    'base_url:"/data/oms/shipment/bulk/invoice/provider/allocate" OR '
    'base_url:"/services/rest/v1/oms/shippingPackage/createInvoiceAndGenerateLabel"'
)

BULK_URL = 'base_url:"/data/oms/invoice/v2/bulk/create"'


# ── Elasticsearch helpers ──────────────────────────────────────
def meta():
    return json.dumps({"search_type": "query_then_fetch", "ignore_unavailable": True, "index": "access*"})

def time_filter():
    return {"range": {"@timestamp": {"gte": gte, "lte": lte, "format": "epoch_millis"}}}

def qs(query):
    return {"query_string": {"analyze_wildcard": True, "query": query}}

def perf_aggs(field="channel_src_code", agg_key="2"):
    return {agg_key: {"terms": {"field": field, "size": 500, "order": {"_key": "desc"}, "min_doc_count": 1}, "aggs": {
        "avg_excl":  {"avg":         {"field": "req_prcoess_excl_script_time"}},
        "p75p90_excl": {"percentiles": {"field": "req_prcoess_excl_script_time", "percents": ["75", "90"]}},
        "avg_total": {"avg":         {"field": "req_process_ms"}},
        "p75p90_total": {"percentiles": {"field": "req_process_ms", "percents": ["75", "90"]}}
    }}}

def count_aggs(field="channel_src_code", agg_key="2"):
    return {agg_key: {"terms": {"field": field, "size": 500, "order": {"_key": "desc"}, "min_doc_count": 1}, "aggs": {}}}

def body(query, aggs):
    return json.dumps({"size": 0, "query": {"bool": {"filter": [time_filter(), qs(query)]}}, "aggs": aggs})

def ndjson(*pairs):
    return "\n".join(meta() + "\n" + body(q, a) for q, a in pairs) + "\n"

def call_es(payload):
    r = requests.post(ES_URL, headers=HEADERS, data=payload)
    r.raise_for_status()
    return r.json().get("responses", [])

def buckets(resp, key="2"):
    try:
        return resp["aggregations"][key]["buckets"]
    except Exception:
        return []


# ── Query 1: Bulk invoice create ───────────────────────────────
def fetch_bulk():
    payload = ndjson(
        (BULK_URL,                            perf_aggs()),
        (BULK_URL + " AND api_status:SUCCESS",     count_aggs()),
        (BULK_URL + " AND NOT api_status:SUCCESS", count_aggs()),
    )
    rs = call_es(payload)
    return merge_channel(rs, perf_key="2", count_key="2")


# ── Query 2: Allocate shipping ─────────────────────────────────
def fetch_allocate():
    payload = ndjson(
        (f"({ALLOCATE_URLS})",                            perf_aggs()),
        (f"({ALLOCATE_URLS}) AND api_status:SUCCESS",     count_aggs()),
        (f"({ALLOCATE_URLS}) AND NOT api_status:SUCCESS", count_aggs()),
    )
    rs = call_es(payload)
    return merge_channel(rs, perf_key="2", count_key="2")


# ── Query 3: Invoice create — error breakdown ──────────────────
def fetch_invoice_errors():
    base = f"({INVOICE_URLS}) AND NOT channel_src_code:\"-\""
    payload = ndjson(
        (base,                                                                        count_aggs()),
        (base + " AND api_status:SUCCESS",                                            count_aggs()),
        (base + " AND NOT api_status:SUCCESS",                                        count_aggs()),
        (base + " AND NOT api_status:SUCCESS AND error_response_message:*SCRIPT_EXECUTION_FAILED*",  count_aggs()),
        (base + " AND NOT api_status:SUCCESS AND error_response_message:*GSTIN_EINVOICE_ERROR*",     count_aggs()),
        (base + " AND NOT api_status:SUCCESS AND NOT error_response_message:*SCRIPT_EXECUTION_FAILED* AND NOT error_response_message:*GSTIN_EINVOICE_ERROR*", count_aggs()),
    )
    rs = call_es(payload)
    result = {}
    for b in buckets(rs[0]):
        result[b["key"]] = {"total": b["doc_count"], "success": 0, "failure": 0, "script_fail": 0, "gstin_fail": 0, "other_fail": 0}
    for b in buckets(rs[1]):
        if b["key"] in result: result[b["key"]]["success"]     = b["doc_count"]
    for b in buckets(rs[2]):
        if b["key"] in result: result[b["key"]]["failure"]     = b["doc_count"]
    for b in buckets(rs[3]):
        if b["key"] in result: result[b["key"]]["script_fail"] = b["doc_count"]
    for b in buckets(rs[4]):
        if b["key"] in result: result[b["key"]]["gstin_fail"]  = b["doc_count"]
    for b in buckets(rs[5]):
        if b["key"] in result: result[b["key"]]["other_fail"]  = b["doc_count"]
    return result


# ── Query 4: Invoice create — performance ─────────────────────
def fetch_invoice_perf():
    base = f"({INVOICE_URLS}) AND NOT channel_src_code:\"-\""
    payload = ndjson(
        (base,                            perf_aggs()),
        (base + " AND api_status:SUCCESS",     count_aggs()),
        (base + " AND NOT api_status:SUCCESS", count_aggs()),
    )
    rs = call_es(payload)
    return merge_channel(rs, perf_key="2", count_key="2")


# ── Query 5: Per-tenant performance ───────────────────────────
def fetch_tenant_perf():
    base = f"({INVOICE_URLS}) AND NOT channel_src_code:\"-\""
    tenant_aggs = {"4": {"terms": {"field": "tenant", "size": 500, "order": {"_key": "desc"}, "min_doc_count": 1}, "aggs": {
        "9": {"terms": {"field": "channel_src_code", "size": 500, "order": {"_key": "desc"}, "min_doc_count": 1}, "aggs": {
            "avg_excl":    {"avg":         {"field": "req_prcoess_excl_script_time"}},
            "p75p90_excl": {"percentiles": {"field": "req_prcoess_excl_script_time", "percents": ["75", "90"]}},
            "avg_total":   {"avg":         {"field": "req_process_ms"}},
            "p75p90_total":{"percentiles": {"field": "req_process_ms", "percents": ["75", "90"]}}
        }}
    }}}
    payload = meta() + "\n" + body(base, tenant_aggs) + "\n"
    rs = call_es(payload)
    # Return top 5 slowest tenant+channel combos
    results = []
    for tb in buckets(rs[0], "4"):
        tenant = tb["key"]
        for cb in tb.get("9", {}).get("buckets", []):
            p90 = (cb.get("p75p90_total") or {}).get("values", {}).get("90.0") or 0
            results.append({"tenant": tenant, "channel": cb["key"], "p90": round(p90, 0)})
    results.sort(key=lambda x: x["p90"], reverse=True)
    return results[:10]


# ── Query 6: Error message breakdown ──────────────────────────
def fetch_error_breakdown():
    base = f"({INVOICE_URLS})"
    err_aggs = {"4": {"terms": {"field": "channel_src_code", "size": 500, "order": {"_key": "desc"}, "min_doc_count": 1}, "aggs": {
        "5": {"terms": {"field": "error_response_message", "size": 500, "order": {"_key": "desc"}, "min_doc_count": 1}, "aggs": {}}
    }}}
    payload = meta() + "\n" + body(base, err_aggs) + "\n"
    rs = call_es(payload)
    result = {}
    for cb in buckets(rs[0], "4"):
        ch = cb["key"]
        errors = []
        for eb in cb.get("5", {}).get("buckets", []):
            if eb["key"] != "-":
                errors.append({"msg": eb["key"][:80], "count": eb["doc_count"]})
        errors.sort(key=lambda x: x["count"], reverse=True)
        result[ch] = errors[:5]
    return result


# ── Merge perf + success/failure counts ───────────────────────
def merge_channel(rs, perf_key="2", count_key="2"):
    data = {}
    for b in buckets(rs[0], perf_key):
        ch = b["key"]
        p75p90 = (b.get("p75p90_total") or {}).get("values", {})
        excl   = (b.get("p75p90_excl")  or {}).get("values", {})
        data[ch] = {
            "total":            b["doc_count"],
            "success":          0,
            "failure":          0,
            "failure_rate":     0,
            "avg_total":        round(b.get("avg_total", {}).get("value") or 0, 1),
            "p75_total":        round(p75p90.get("75.0") or 0, 1),
            "p90_total":        round(p75p90.get("90.0") or 0, 1),
            "avg_excl":         round(b.get("avg_excl",  {}).get("value") or 0, 1),
            "p75_excl":         round(excl.get("75.0") or 0, 1),
            "p90_excl":         round(excl.get("90.0") or 0, 1),
        }
    for b in buckets(rs[1], count_key):
        if b["key"] in data: data[b["key"]]["success"] = b["doc_count"]
    for b in buckets(rs[2], count_key):
        if b["key"] in data: data[b["key"]]["failure"] = b["doc_count"]
    for ch, d in data.items():
        d["failure_rate"] = round((d["failure"] / d["total"]) * 100, 2) if d["total"] else 0
    return data


# ── Threshold evaluation ───────────────────────────────────────
def find_alerts(label, channel_data):
    alerts = []
    for ch, d in channel_data.items():
        if d["failure_rate"] > FAILURE_RATE_THRESHOLD:
            alerts.append({"label": label, "channel": ch, "type": "HIGH_FAILURE_RATE",
                           "value": f"{d['failure_rate']}%", "threshold": f"{FAILURE_RATE_THRESHOLD}%",
                           "total": d["total"], "failure": d["failure"]})
        if d["p90_total"] > P90_THRESHOLD:
            alerts.append({"label": label, "channel": ch, "type": "HIGH_P90_TIME",
                           "value": f"{d['p90_total']}ms", "threshold": f"{P90_THRESHOLD}ms"})
    return alerts


# ── Build Slack blocks ─────────────────────────────────────────
def build_slack_payload(alerts, bulk, allocate, inv_perf, inv_errors, tenant_perf, err_breakdown):
    blocks = []

    # ── Header ──
    has_alerts = len(alerts) > 0
    header_text = f":rotating_light:  Invoice Monitor Alert — {date_str}" if has_alerts else f":white_check_mark:  Invoice Monitor — {date_str} — All Clear"
    blocks.append({"type": "header", "text": {"type": "plain_text", "text": header_text, "emoji": True}})
    blocks.append({"type": "divider"})

    # ── Alert summary ──
    if has_alerts:
        by_ch = {}
        for a in alerts:
            by_ch.setdefault(a["channel"], []).append(a)

        for ch, ch_alerts in by_ch.items():
            lines = [f"*Channel: `{ch}`*"]
            for a in ch_alerts:
                icon = ":x:" if a["type"] == "HIGH_FAILURE_RATE" else ":hourglass_flowing_sand:"
                lines.append(f"{icon} *{a['type']}* [{a['label']}] — *{a['value']}* (threshold: {a['threshold']})")
                if a["type"] == "HIGH_FAILURE_RATE":
                    lines.append(f"   └ {a['failure']} failures out of {a['total']} total hits")
            if err_breakdown.get(ch):
                lines.append("\n:bar_chart: *Top Errors:*")
                for e in err_breakdown[ch]:
                    lines.append(f"  • `{e['msg']}` — {e['count']} hits")
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
            blocks.append({"type": "divider"})

    # ── Invoice Create Performance table ──
    if inv_perf:
        rows = ["*:page_facing_up: Invoice Create — Channel Performance*",
                "```",
                f"{'Channel':<22} {'Total':>7} {'Succ':>6} {'Fail':>6} {'Fail%':>6} {'AvgMs':>7} {'P75':>7} {'P90':>7}",
                "─" * 72]
        for ch, d in sorted(inv_perf.items()):
            rows.append(f"{ch:<22} {d['total']:>7} {d['success']:>6} {d['failure']:>6} {d['failure_rate']:>5}% {d['avg_total']:>7} {d['p75_total']:>7} {d['p90_total']:>7}")
        rows.append("```")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(rows)}})
        blocks.append({"type": "divider"})

    # ── Bulk Invoice Create ──
    if bulk:
        rows = ["*:package: Bulk Invoice Create — Channel Summary*", "```",
                f"{'Channel':<22} {'Total':>7} {'Succ':>6} {'Fail':>6} {'Fail%':>6} {'P90ms':>7}",
                "─" * 60]
        for ch, d in sorted(bulk.items()):
            rows.append(f"{ch:<22} {d['total']:>7} {d['success']:>6} {d['failure']:>6} {d['failure_rate']:>5}% {d['p90_total']:>7}")
        rows.append("```")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(rows)}})
        blocks.append({"type": "divider"})

    # ── Allocate Shipping ──
    if allocate:
        rows = ["*:truck: Allocate Shipping — Channel Summary*", "```",
                f"{'Channel':<22} {'Total':>7} {'Succ':>6} {'Fail':>6} {'Fail%':>6} {'P90ms':>7}",
                "─" * 60]
        for ch, d in sorted(allocate.items()):
            rows.append(f"{ch:<22} {d['total']:>7} {d['success']:>6} {d['failure']:>6} {d['failure_rate']:>5}% {d['p90_total']:>7}")
        rows.append("```")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(rows)}})
        blocks.append({"type": "divider"})

    # ── Invoice error breakdown (script / gstin / other) ──
    if inv_errors:
        rows = ["*:warning: Invoice Create — Error Breakdown*", "```",
                f"{'Channel':<22} {'Total':>7} {'ScriptFail':>11} {'GSTINFail':>10} {'OtherFail':>10}",
                "─" * 65]
        for ch, d in sorted(inv_errors.items()):
            rows.append(f"{ch:<22} {d['total']:>7} {d['script_fail']:>11} {d['gstin_fail']:>10} {d['other_fail']:>10}")
        rows.append("```")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(rows)}})
        blocks.append({"type": "divider"})

    # ── Top slow tenants ──
    if tenant_perf:
        rows = ["*:snail: Top 10 Slowest Tenant × Channel (P90)*", "```",
                f"{'Tenant':<30} {'Channel':<22} {'P90ms':>7}", "─" * 62]
        for t in tenant_perf:
            flag = " :rotating_light:" if t["p90"] > P90_THRESHOLD else ""
            rows.append(f"{t['tenant']:<30} {t['channel']:<22} {t['p90']:>7}{flag}")
        rows.append("```")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(rows)}})
        blocks.append({"type": "divider"})

    # ── Footer ──
    blocks.append({"type": "context", "elements": [
        {"type": "mrkdwn", "text": f":link: <https://uniwatch.unicommerce.com/db/d/NoQi9W2SK/invoice_data_performance_peak?orgId=1&from=now-1d&to=now|View Grafana Dashboard>  |  Thresholds: Failure >{FAILURE_RATE_THRESHOLD}%  P90 >{P90_THRESHOLD}ms"}
    ]})

    return {"blocks": blocks}


# ── Post to Slack ──────────────────────────────────────────────
def post_slack(payload):
    r = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if r.status_code != 200:
        raise Exception(f"Slack error {r.status_code}: {r.text}")
    print("Slack message sent successfully.")


# ── Main ───────────────────────────────────────────────────────
def main():
    print(f"Fetching data for {date_str} (gte={gte}, lte={lte})")

    bulk        = fetch_bulk()
    allocate    = fetch_allocate()
    inv_errors  = fetch_invoice_errors()
    inv_perf    = fetch_invoice_perf()
    tenant_perf = fetch_tenant_perf()
    err_breakdown = fetch_error_breakdown()

    alerts = []
    alerts += find_alerts("BulkInvoiceCreate", bulk)
    alerts += find_alerts("AllocateShipping",  allocate)
    alerts += find_alerts("InvoiceCreate",     inv_perf)

    print(f"Alerts triggered: {len(alerts)}")
    payload = build_slack_payload(alerts, bulk, allocate, inv_perf, inv_errors, tenant_perf, err_breakdown)
    post_slack(payload)


if __name__ == "__main__":
    main()
