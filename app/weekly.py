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

    # Placement analysis (B-Full M1) — aggregate 14d, classify, inject into grouped
    _inject_placement(grouped, t_14, end)

    # Targeting analysis (B-Full M2) — keyword + ASIN level, high/low CVR + match-type
    _inject_targeting(grouped, t_14, end)

    return {
        "report_date": end.isoformat(),
        "windows": {"t_14": t_14.isoformat(), "t_30": t_30.isoformat(), "t_60": t_60.isoformat()},
        "store_count": _store_count(grouped),
        "owner_results": grouped,
        "stats": _stats(grouped),
    }


HIGH_CVR = 8.0   # Bella threshold: CVR ≥ 8% counts as 高效
LOW_CVR = 1.0    # CVR < 1% counts as 低效
HIGH_COST = 5.0  # 花费 ≥ $5 才纳入分类（避免噪声）


def _calc_kw(r):
    clk = int(r.get("clicks") or 0)
    cost = float(r.get("cost") or 0)
    ord_ = int(r.get("orders") or 0)
    sales = float(r.get("sales") or 0)
    r["clicks"] = clk
    r["cost"] = cost
    r["orders"] = ord_
    r["sales"] = sales
    r["impressions"] = int(r.get("impressions") or 0)
    r["cvr"] = round(ord_ / clk * 100, 2) if clk else 0
    r["acos"] = round(cost / sales * 100, 2) if sales else 999
    r["cpc"] = round(cost / clk, 2) if clk else 0
    r["ctr"] = round(r["clicks"] / r["impressions"] * 100, 2) if r["impressions"] else 0
    return r


def _inject_targeting(grouped: dict, t_14, end):
    """Build targeting structure per (owner, store) for N5 sr.targeting rendering.
    Source: keyword-level (spKeywordReports) + target-level (spTargetReports), 14d window."""
    kw_raw = db.aggregate_targeting_kw_14d(t_14, end)
    tgt_raw = db.aggregate_targeting_tgt_14d(t_14, end)

    by_os_kw = {}
    for r in kw_raw:
        owner = r.get("owner") or "未分配"
        store = r.get("store_name") or ""
        by_os_kw.setdefault((owner, store), []).append(_calc_kw(dict(r)))

    by_os_tgt = {}
    for r in tgt_raw:
        owner = r.get("owner") or "未分配"
        store = r.get("store_name") or ""
        by_os_tgt.setdefault((owner, store), []).append(_calc_kw(dict(r)))

    # union of (owner, store) keys
    all_keys = set(by_os_kw.keys()) | set(by_os_tgt.keys())
    for (owner, store) in all_keys:
        if owner == "未分配":
            continue
        if owner not in grouped or store not in grouped[owner]:
            continue
        kw_rows = by_os_kw.get((owner, store), [])
        tgt_rows = by_os_tgt.get((owner, store), [])

        # 高效/低效 keyword (Bella: CVR≥8% + 花费≥$5)
        high_kw = sorted(
            [r for r in kw_rows if r["cvr"] >= HIGH_CVR and r["cost"] >= HIGH_COST],
            key=lambda x: -x["cvr"]
        )[:15]
        low_kw = sorted(
            [r for r in kw_rows if r["cvr"] < LOW_CVR and r["cost"] >= HIGH_COST],
            key=lambda x: -x["cost"]
        )[:10]

        # 高效/低效 target (ASIN/品类定位)
        high_tgt = sorted(
            [r for r in tgt_rows if r["cvr"] >= HIGH_CVR and r["cost"] >= HIGH_COST],
            key=lambda x: -x["cvr"]
        )[:15]
        low_tgt = sorted(
            [r for r in tgt_rows if r["cvr"] < LOW_CVR and r["cost"] >= HIGH_COST],
            key=lambda x: -x["cost"]
        )[:10]

        # 匹配类型对比 (mirror v1 N3 analyzeTgt mtAgg logic)
        mt_agg = {}
        for r in kw_rows:
            mt = r.get("match_type") or "UNKNOWN"
            if mt not in mt_agg:
                mt_agg[mt] = {"match_type": mt, "count": 0, "impressions": 0,
                              "clicks": 0, "cost": 0.0, "orders": 0, "sales": 0.0}
            mt_agg[mt]["count"] += 1
            mt_agg[mt]["impressions"] += r["impressions"]
            mt_agg[mt]["clicks"] += r["clicks"]
            mt_agg[mt]["cost"] += r["cost"]
            mt_agg[mt]["orders"] += r["orders"]
            mt_agg[mt]["sales"] += r["sales"]
        mt_list = []
        for m in mt_agg.values():
            m["cvr"] = round(m["orders"] / m["clicks"] * 100, 2) if m["clicks"] else 0
            m["acos"] = round(m["cost"] / m["sales"] * 100, 2) if m["sales"] else 999
            m["cpc"] = round(m["cost"] / m["clicks"], 2) if m["clicks"] else 0
            mt_list.append(m)

        # 建议 (mirror v1 N3 logic)
        sug = []
        total_cost = sum(m["cost"] for m in mt_list)
        total_sales = sum(m["sales"] for m in mt_list)
        for m in mt_list:
            if m["acos"] > 80 and m["cost"] > 10:
                sug.append(f"{m['match_type']}: ACOS={m['acos']:.0f}%过高，建议降竞价或停投")
            if m["cvr"] < 2 and m["clicks"] > 50:
                sug.append(f"{m['match_type']}: CVR={m['cvr']}%过低，检查流量是否过杂")
            cp = m["cost"] / total_cost * 100 if total_cost else 0
            sp = m["sales"] / total_sales * 100 if total_sales else 0
            if cp > 50 and sp < 30:
                sug.append(f"{m['match_type']}: 花费占{cp:.0f}%但销售占{sp:.0f}%，建议重分预算")
        e_mt = next((m for m in mt_list if m["match_type"] == "EXACT"), None)
        b_mt = next((m for m in mt_list if m["match_type"] == "BROAD"), None)
        if e_mt and b_mt and e_mt["cpc"] > 0 and b_mt["cpc"] > 0 and e_mt["cpc"] > b_mt["cpc"] * 1.5:
            sug.append(f"EXACT CPC=${e_mt['cpc']:.2f}远高于BROAD CPC=${b_mt['cpc']:.2f}，检查EXACT竞价是否过激")

        # Build v1 N5 schema row mappers (alias clicks→clicks, etc., target_text from kw_text or expression)
        def _kw_render(r):
            return {
                "target_text": r.get("keyword_text", "") or str(r.get("keyword_id", "")),
                "keyword_text": r.get("keyword_text", ""),
                "match_type": r.get("match_type", ""),
                "target_id": r.get("keyword_id", ""),
                "cvr": r["cvr"], "clicks": r["clicks"], "cost": r["cost"],
                "orders": r["orders"], "sales": r["sales"], "cpc": r["cpc"],
                "campaign_id": r.get("campaign_id"),
            }

        def _tgt_render(r):
            return {
                "target_text": r.get("targeting_expression", "") or str(r.get("target_id", "")),
                "target_id": r.get("target_id", ""),
                "cvr": r["cvr"], "clicks": r["clicks"], "cost": r["cost"],
                "orders": r["orders"], "sales": r["sales"], "cpc": r["cpc"],
                "campaign_id": r.get("campaign_id"),
            }

        grouped[owner][store]["targeting"] = {
            "highEffKw": [_kw_render(r) for r in high_kw],
            "lowEffKw": [_kw_render(r) for r in low_kw],
            "highEffTgt": [_tgt_render(r) for r in high_tgt],
            "lowEffTgt": [_tgt_render(r) for r in low_tgt],
            "matchTypeCompare": mt_list,
            "matchTypeSuggestions": sug,
        }


def _inject_placement(grouped: dict, t_14, end):
    """Build placement structure per (owner, store) for N5 sr.placement rendering."""
    raw = db.aggregate_placement_14d(t_14, end)
    # raw rows: {owner, store_name, sid, placement_type, impressions, clicks, cost, orders, sales}
    by_os = {}  # (owner, store) → [rows]
    for r in raw:
        owner = r["owner"] or "未分配"
        store = r["store_name"] or ""
        by_os.setdefault((owner, store), []).append(r)

    for (owner, store), rows in by_os.items():
        if owner == "未分配":
            continue
        if owner not in grouped or store not in grouped[owner]:
            continue
        items = []
        total_clicks = 0
        total_orders = 0
        for r in rows:
            clk = int(r.get("clicks") or 0)
            ord_ = int(r.get("orders") or 0)
            cost = float(r.get("cost") or 0)
            sales = float(r.get("sales") or 0)
            cvr = round(ord_ / clk * 100, 2) if clk else 0
            acos = round(cost / sales * 100, 2) if sales else 999
            items.append({
                "placement_type": r["placement_type"] or "UNKNOWN",
                "impressions": int(r.get("impressions") or 0),
                "clicks": clk,
                "cost": cost,
                "orders": ord_,
                "sales": sales,
                "cvr": cvr,
                "acos": acos,
            })
            total_clicks += clk
            total_orders += ord_

        avg_cvr = round(total_orders / total_clicks * 100, 2) if total_clicks else 0

        # Multiplier suggestions (mirror v1 N3 analyzePlc logic)
        suggestions = []
        for p in items:
            if p["clicks"] < 30:
                suggestions.append({
                    "placement_type": p["placement_type"], "cvr": p["cvr"],
                    "ratio": 0, "suggestion": "数据不足",
                })
                continue
            ratio = round(p["cvr"] / avg_cvr, 2) if avg_cvr else 0
            if ratio >= 2.0:
                s = "+100%"
            elif ratio >= 1.5:
                s = "+50%"
            elif ratio >= 1.2:
                s = "+25%"
            elif ratio <= 0.5:
                s = "降竞价或关停"
            else:
                s = "不调整"
            suggestions.append({
                "placement_type": p["placement_type"], "cvr": p["cvr"],
                "ratio": ratio, "suggestion": s,
            })

        best = None
        eligible = [p for p in items if p["clicks"] >= 30]
        if eligible:
            best_p = max(eligible, key=lambda x: x["cvr"])
            best = {"placement_type": best_p["placement_type"], "cvr": best_p["cvr"]}

        grouped[owner][store]["placement"] = {
            "items": items,
            "avgCvr": avg_cvr,
            "multiplierSuggestions": suggestions,
            "bestPlacement": best,
        }


# legacy sync wrapper for any non-async callers
def build_weekly_data(end_date: date = None, t14: int = 14, t30: int = 30, t60: int = 60,
                     offset_days: int = 8):
    return asyncio.run(build_weekly_data_async(end_date, t14, t30, t60, offset_days))


async def _enrich_campaign_names_async(rows: list) -> list:
    """Build (sid, campaign_id) → name map by calling spCampaigns per sid in PARALLEL, then inject."""
    sids = list({int(r["sid"]) for r in rows if r.get("sid")})

    async def _fetch_one(sid):
        try:
            camps = await lingxing.sp_campaigns(sid)
            out = {}
            for c in camps:
                cid = c.get("campaign_id")
                nm = c.get("name") or ""
                if cid is not None and nm:
                    try:
                        out[(sid, int(cid))] = nm
                    except (TypeError, ValueError):
                        pass
            return out
        except Exception:
            return {}

    # Run all sids in parallel (max ~20 — within Lingxing rate limit)
    results = await asyncio.gather(*(_fetch_one(s) for s in sids))
    name_map = {}
    for m in results:
        name_map.update(m)

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
