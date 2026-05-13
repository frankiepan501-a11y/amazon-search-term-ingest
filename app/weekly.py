"""/weekly-data orchestrator: pull aggregates + last-week recs + neg snapshot + bid history,
then run classify + group → return owner-keyed structured payload for n8n N5 renderer.
"""
import asyncio
from datetime import date, timedelta
from . import db, analysis, lingxing


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _row_to_safe(r):
    """Convert Decimal/Date in dict-row to JSON-friendly."""
    out = {}
    for k, v in r.items():
        if hasattr(v, "is_finite"):
            out[k] = float(v)
        elif isinstance(v, date):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


async def build_weekly_data_async(end_date: date = None, t14: int = 14, t30: int = 30, t60: int = 60,
                                   offset_days: int = 8):
    """Async main entry. FastAPI handler awaits this."""
    end = end_date or (date.today() - timedelta(days=offset_days))
    start = end - timedelta(days=t60 - 1)
    t_14 = end - timedelta(days=t14 - 1)
    t_30 = end - timedelta(days=t30 - 1)
    t_60 = end - timedelta(days=t60 - 1)

    raw = db.aggregate_windows(start, end, t_14, t_30, t_60)
    rows = [_row_to_safe(r) for r in raw]

    rows = await _enrich_campaign_names_async(rows)

    last_week_recs = _load_last_week_recs(end)
    neg_set = _load_neg_snapshot(end)
    classified = analysis.classify_rows(rows, neg_set, last_week_recs, {})
    grouped = analysis.group_by_owner_store(classified)
    _persist_new_recs(grouped, end)

    return {
        "report_date": end.isoformat(),
        "windows": {"t_14": t_14.isoformat(), "t_30": t_30.isoformat(), "t_60": t_60.isoformat()},
        "store_count": _store_count(grouped),
        "owner_results": grouped,
        "stats": _stats(grouped),
    }


# legacy sync wrapper for any non-async callers
def build_weekly_data(end_date: date = None, t14: int = 14, t30: int = 30, t60: int = 60,
                     offset_days: int = 8):
    return asyncio.run(build_weekly_data_async(end_date, t14, t30, t60, offset_days))


async def _enrich_campaign_names_async(rows: list) -> list:
    """Build (sid, campaign_id) → name map by calling spCampaigns per sid, then inject."""
    sids = {int(r["sid"]) for r in rows if r.get("sid")}
    name_map = {}
    for sid in sids:
        try:
            camps = await lingxing.sp_campaigns(sid)
            for c in camps:
                cid = c.get("campaign_id")
                nm = c.get("name") or ""
                if cid is not None and nm:
                    try:
                        name_map[(sid, int(cid))] = nm
                    except (TypeError, ValueError):
                        pass
        except Exception:
            pass
        await asyncio.sleep(0.3)
    for r in rows:
        cid = r.get("campaign_id"); sid = r.get("sid")
        if cid and sid:
            try:
                nm = name_map.get((int(sid), int(cid)))
                if nm:
                    r["campaign_name"] = nm
            except (TypeError, ValueError):
                pass
    return rows


def _load_last_week_recs(report_date: date) -> dict:
    last = report_date - timedelta(days=7)
    sql = """SELECT sid, query, campaign_id, recommendation, reason, bid_at_advice
             FROM amazon_ads.search_term_recommendation WHERE report_date = %s"""
    with db.conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, (last,))
            out = {}
            for r in cur.fetchall():
                key = (r["sid"], r["query"], r["campaign_id"] or 0)
                out[key] = {
                    "recommendation": r["recommendation"], "reason": r["reason"],
                    "bid_at_advice": _safe_float(r["bid_at_advice"]),
                }
            return out


def _load_neg_snapshot(report_date: date) -> set:
    """Set of (sid, lowercased neg_text) within last 3 days of report_date."""
    sql = """SELECT sid, neg_text FROM amazon_ads.neg_keyword_snapshot
             WHERE snapshot_date >= %s AND snapshot_date <= %s"""
    start = report_date - timedelta(days=3)
    end = report_date + timedelta(days=3)
    with db.conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, (start, end))
            return {(r["sid"], (r["neg_text"] or "").lower()) for r in cur.fetchall()}


def _persist_new_recs(grouped: dict, report_date: date):
    """Insert this week's recommendations for next-week diff."""
    rows = []
    for owner, stores in grouped.items():
        for store, d in stores.items():
            for r in d["negate"]:
                rows.append((report_date, r["sid"], r["store_name"], owner, r["query"],
                             r.get("campaign_id") or 0, r.get("match_type") or "",
                             "否定", r.get("reason", ""), r.get("bid_latest")))
            # boost/scale → "加预算"; observe → skip
            for r in d["boost"] + d["scale"]:
                rows.append((report_date, r["sid"], r["store_name"], owner, r["query"],
                             r.get("campaign_id") or 0, r.get("match_type") or "",
                             "加预算", r.get("reason", ""), r.get("bid_latest")))
    if not rows:
        return
    with db.conn() as c:
        with c.cursor() as cur:
            cur.executemany(
                """INSERT INTO amazon_ads.search_term_recommendation
                   (report_date, sid, store_name, owner, query, campaign_id, match_type,
                    recommendation, reason, bid_at_advice, status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                   ON CONFLICT (report_date, sid, query, campaign_id, match_type, recommendation)
                   DO UPDATE SET reason=EXCLUDED.reason, bid_at_advice=EXCLUDED.bid_at_advice""",
                rows)


def _store_count(grouped):
    n = 0
    for o, stores in grouped.items():
        n += len(stores)
    return n


def _stats(grouped):
    s = {"scale": 0, "boost": 0, "negate": 0, "warn": 0, "pending_human": 0, "observe": 0}
    for o, stores in grouped.items():
        for store, d in stores.items():
            for k in s:
                s[k] += len(d.get(k, []))
    return s
