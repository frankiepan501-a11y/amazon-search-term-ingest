"""Single-day fetch logic: pull search-term + bid + meta for one (sid, date), build upsert rows."""
import re
import asyncio
from . import lingxing

_ASIN_RE = re.compile(r"^[Bb]0[A-Za-z0-9]{8}$")


def _is_asin(q):
    return bool(_ASIN_RE.match((q or "").strip()))


def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _i(x, default=0):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


async def fetch_seller_meta(sid: int):
    """campaign_id -> asin, asin -> owner. Cached per-fetch."""
    ads, lst = await asyncio.gather(lingxing.sp_product_ads(sid), lingxing.listings(sid))
    c_to_asin = {}
    c_to_name = {}
    for p in ads:
        cid = p.get("campaign_id")
        if cid:
            if p.get("asin"):
                c_to_asin[cid] = p["asin"]
            if p.get("campaign_name"):
                c_to_name[cid] = p["campaign_name"]
    asin_to_owner = {}
    for li in lst:
        asin = li.get("asin") or ""
        pi = li.get("principal_info") or []
        pn = pi[0].get("principal_name") if pi else ""
        if asin and pn:
            asin_to_owner[asin] = pn
    return c_to_asin, c_to_name, asin_to_owner


async def fetch_bid_map(sid: int, report_date: str):
    """keyword_id -> bid. spKeywordReports is the only source that carries bid."""
    rows = await lingxing.sp_keyword_reports(sid, report_date)
    bid_map = {}
    for r in rows:
        kid = r.get("keyword_id") or r.get("target_id")
        bid = r.get("bid") or r.get("base_bid")
        if kid and bid is not None:
            try:
                bid_map[str(kid)] = float(bid)
            except (TypeError, ValueError):
                pass
    return bid_map


async def fetch_day_rows(sid: int, store_name: str, report_date: str):
    """Return list[dict] ready for upsert + count summary."""
    kw_rows, tgt_rows, sb_rows = await asyncio.gather(
        lingxing.query_word_keyword(sid, report_date),
        lingxing.query_word_target(sid, report_date),
        lingxing.hsa_query_word(sid, report_date),
    )
    c_to_asin, c_to_name, asin_to_owner = await fetch_seller_meta(sid)
    bid_map = await fetch_bid_map(sid, report_date)

    out = []
    for r, src in [(kw_rows, "sp_kw"), (tgt_rows, "sp_tgt"), (sb_rows, "sb_kw")]:
        for x in r:
            q = (x.get("query") or "").strip()
            if not q or _is_asin(q):
                continue
            cid = x.get("campaign_id") or 0
            asin = c_to_asin.get(cid, "")
            owner = asin_to_owner.get(asin, "未分配")
            kid = x.get("keyword_id") or x.get("target_id")
            bid = bid_map.get(str(kid)) if kid else None
            out.append({
                "sid": sid,
                "store_name": store_name,
                "report_date": report_date,
                "query": q,
                "campaign_id": cid or 0,
                "campaign_name": c_to_name.get(cid, ""),
                "target_text": x.get("target_text") or "",
                "match_type": x.get("match_type") or "",
                "asin": asin,
                "owner": owner,
                "impressions": _i(x.get("impressions")),
                "clicks": _i(x.get("clicks")),
                "cost": _f(x.get("cost")),
                "orders": _i(x.get("orders")),
                "same_orders": _i(x.get("same_orders")),
                "sales": _f(x.get("sales")),
                "same_sales": _f(x.get("same_sales")),
                "units": _i(x.get("units")),
                "bid": bid,
                "source_api": src,
            })
    return out


async def fetch_targeting_day(sid: int, store_name: str, report_date: str):
    """Pull spKeywordReports + spTargetReports for one day. Returns (kw_rows, tgt_rows)."""
    kw_raw, tgt_raw = await asyncio.gather(
        lingxing.sp_keyword_reports(sid, report_date),
        lingxing.sp_target_reports(sid, report_date),
    )
    c_to_asin, _c_to_name, asin_to_owner = await fetch_seller_meta(sid)

    def _owner_for(cid):
        asin = c_to_asin.get(cid, "")
        return asin_to_owner.get(asin, "未分配")

    kw_rows = []
    for r in kw_raw:
        cid = r.get("campaign_id") or 0
        kw_rows.append({
            "sid": sid, "store_name": store_name, "report_date": report_date,
            "keyword_id": r.get("keyword_id") or 0,
            "keyword_text": r.get("keyword_text") or "",
            "match_type": r.get("match_type") or "",
            "ad_group_id": r.get("ad_group_id") or 0,
            "campaign_id": cid or 0,
            "owner": _owner_for(cid),
            "impressions": _i(r.get("impressions")),
            "clicks": _i(r.get("clicks")),
            "cost": _f(r.get("cost")),
            "orders": _i(r.get("orders")),
            "sales": _f(r.get("sales")),
            "units": _i(r.get("units")),
        })

    tgt_rows = []
    for r in tgt_raw:
        cid = r.get("campaign_id") or 0
        tgt_rows.append({
            "sid": sid, "store_name": store_name, "report_date": report_date,
            "target_id": r.get("target_id") or 0,
            "targeting_expression": r.get("targeting_expression") or "",
            "targeting_type": r.get("targeting_type") or "",
            "ad_group_id": r.get("ad_group_id") or 0,
            "campaign_id": cid or 0,
            "owner": _owner_for(cid),
            "impressions": _i(r.get("impressions")),
            "clicks": _i(r.get("clicks")),
            "cost": _f(r.get("cost")),
            "orders": _i(r.get("orders")),
            "sales": _f(r.get("sales")),
            "units": _i(r.get("units")),
        })

    return kw_rows, tgt_rows


async def fetch_placement_day(sid: int, store_name: str, report_date: str):
    """Pull campaignPlacementReports for one day, map campaign→ASIN→owner."""
    rows = await lingxing.campaign_placement_reports(sid, report_date)
    c_to_asin, c_to_name, asin_to_owner = await fetch_seller_meta(sid)
    out = []
    for r in rows:
        cid = r.get("campaign_id") or 0
        asin = c_to_asin.get(cid, "")
        owner = asin_to_owner.get(asin, "未分配")
        out.append({
            "sid": sid,
            "store_name": store_name,
            "report_date": report_date,
            "campaign_id": cid or 0,
            "placement_type": r.get("placement_type") or "UNKNOWN",
            "owner": owner,
            "impressions": _i(r.get("impressions")),
            "clicks": _i(r.get("clicks")),
            "cost": _f(r.get("cost")),
            "orders": _i(r.get("orders")),
            "sales": _f(r.get("sales")),
        })
    return out


async def fetch_active_sellers():
    sellers = await lingxing.list_sellers()
    # filter to active = has data in any newad endpoint today? Use status/seller_status if present
    active = []
    for s in sellers:
        if s.get("status") in (None, 1, "1") and s.get("sid"):
            active.append({"sid": int(s["sid"]), "name": s.get("name") or s.get("seller_name") or f"sid-{s['sid']}"})
    return active
