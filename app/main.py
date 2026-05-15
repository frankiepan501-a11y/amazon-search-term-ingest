import asyncio
import logging
import os
from datetime import date, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
from . import db, fetcher, feishu, config, weekly

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("amazon-search-term")

app = FastAPI(title="Amazon Search Term Daily Ingest")

# track consecutive daily failures for escalation rule (Q6)
_state = {"consecutive_failures": 0}


def _auth(token: Optional[str]):
    if not config.API_BEARER_TOKEN:
        return  # auth disabled (dev only)
    if not token or token != f"Bearer {config.API_BEARER_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/")
async def root():
    return {"service": "amazon-search-term-daily", "ok": True}


@app.post("/init")
async def init_db(authorization: Optional[str] = Header(None)):
    _auth(authorization)
    r = db.init_schema()
    return r


class DailyRequest(BaseModel):
    date: Optional[str] = None  # YYYY-MM-DD; default = T-2 (BJ)
    sids: Optional[list] = None  # optional filter; default = all active


async def _daily_run(target_date: str, sids_filter):
    try:
        sellers = await fetcher.fetch_active_sellers()
        if sids_filter:
            allow = {int(s) for s in sids_filter}
            sellers = [s for s in sellers if s["sid"] in allow]
        log.info(f"daily bg start date={target_date} sellers={len(sellers)}")
        total_rows = 0
        plc_rows_total = 0
        errors = []
        for s in sellers:
            try:
                rows = await fetcher.fetch_day_rows(s["sid"], s["name"], target_date)
                db.upsert_daily_rows(rows)
                total_rows += len(rows)
            except Exception as e:
                log.exception(f"sid={s['sid']} search-term fail")
                errors.append({"sid": s["sid"], "name": s["name"], "err": f"st:{str(e)[:120]}"})
            try:
                plc = await fetcher.fetch_placement_day(s["sid"], s["name"], target_date)
                db.upsert_placement_rows(plc)
                plc_rows_total += len(plc)
            except Exception as e:
                log.exception(f"sid={s['sid']} placement fail")
                errors.append({"sid": s["sid"], "name": s["name"], "err": f"plc:{str(e)[:120]}"})
            try:
                kw_rows, tgt_rows = await fetcher.fetch_targeting_day(s["sid"], s["name"], target_date)
                db.upsert_targeting_kw_rows(kw_rows)
                db.upsert_targeting_tgt_rows(tgt_rows)
            except Exception as e:
                log.exception(f"sid={s['sid']} targeting fail")
                errors.append({"sid": s["sid"], "name": s["name"], "err": f"tgt:{str(e)[:120]}"})
            await asyncio.sleep(0.5)
        log.info(f"daily bg done date={target_date} st_rows={total_rows} plc_rows={plc_rows_total} errors={len(errors)}")
        if errors and len(errors) >= max(1, len(sellers) // 3):
            _state["consecutive_failures"] += 1
            msg = f"daily {target_date} {len(errors)}/{len(sellers)} 店失败 (连续 {_state['consecutive_failures']} 次): {errors[:3]}"
            await feishu.alert_frankie(msg)
            if _state["consecutive_failures"] >= 3:
                await feishu.alert_group(msg)
        else:
            _state["consecutive_failures"] = 0
    except Exception as e:
        log.exception("daily bg fatal")
        _state["consecutive_failures"] += 1
        await feishu.alert_frankie(f"daily {target_date} FATAL: {str(e)[:300]} (连续 {_state['consecutive_failures']} 次)")
        if _state["consecutive_failures"] >= 3:
            await feishu.alert_group(f"daily {target_date} FATAL 连续 3 次以上: {str(e)[:300]}")


@app.post("/daily")
async def daily(req: DailyRequest, background: BackgroundTasks, authorization: Optional[str] = Header(None)):
    """Fire-and-forget: kick off background ingest, return immediately so n8n cron doesn't hit gateway timeout."""
    _auth(authorization)
    target_date = req.date or (date.today() - timedelta(days=2)).isoformat()
    background.add_task(_daily_run, target_date, req.sids)
    return {"accepted": True, "date": target_date, "sids": req.sids or "all-active"}


@app.post("/daily-sync")
async def daily_sync(req: DailyRequest, authorization: Optional[str] = Header(None)):
    """Synchronous variant for manual testing — waits for completion."""
    _auth(authorization)
    target_date = req.date or (date.today() - timedelta(days=2)).isoformat()
    sellers = await fetcher.fetch_active_sellers()
    if req.sids:
        allow = {int(s) for s in req.sids}
        sellers = [s for s in sellers if s["sid"] in allow]
    total_rows = 0
    per_seller = []
    errors = []
    for s in sellers:
        try:
            rows = await fetcher.fetch_day_rows(s["sid"], s["name"], target_date)
            db.upsert_daily_rows(rows)
            total_rows += len(rows)
            per_seller.append({"sid": s["sid"], "name": s["name"], "rows": len(rows)})
        except Exception as e:
            errors.append({"sid": s["sid"], "name": s["name"], "err": str(e)[:200]})
        await asyncio.sleep(0.5)
    return {"date": target_date, "sellers": len(sellers), "total_rows": total_rows,
            "per_seller": per_seller, "errors": errors}


class BackfillRequest(BaseModel):
    start_date: str  # YYYY-MM-DD inclusive
    end_date: str    # YYYY-MM-DD inclusive
    sids: Optional[list] = None
    sleep_between_days: float = 2.0
    include_placement: bool = True
    include_targeting: bool = True
    placement_only: bool = False
    targeting_only: bool = False  # if True, only backfill targeting (kw + tgt)


async def _backfill_run(start_date: str, end_date: str, sids_filter, sleep_between_days: float,
                          include_placement: bool = True, include_targeting: bool = True,
                          placement_only: bool = False, targeting_only: bool = False):
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        sellers = await fetcher.fetch_active_sellers()
        if sids_filter:
            allow = {int(s) for s in sids_filter}
            sellers = [s for s in sellers if s["sid"] in allow]
        only_mode = "placement_only" if placement_only else ("targeting_only" if targeting_only else "all")
        log.info(f"backfill bg {start}..{end} sellers={len(sellers)} mode={only_mode}")
        cur = start
        while cur <= end:
            ds = cur.isoformat()
            day_total = 0; plc_total = 0; kw_total = 0; tgt_total = 0
            day_errors = []
            for s in sellers:
                if not placement_only and not targeting_only:
                    try:
                        rows = await fetcher.fetch_day_rows(s["sid"], s["name"], ds)
                        db.upsert_daily_rows(rows)
                        day_total += len(rows)
                    except Exception as e:
                        log.exception(f"backfill sid={s['sid']} date={ds} st fail")
                        day_errors.append({"sid": s["sid"], "err": f"st:{str(e)[:100]}"})
                if (include_placement and not targeting_only) or placement_only:
                    try:
                        plc = await fetcher.fetch_placement_day(s["sid"], s["name"], ds)
                        db.upsert_placement_rows(plc)
                        plc_total += len(plc)
                    except Exception as e:
                        log.exception(f"backfill sid={s['sid']} date={ds} plc fail")
                        day_errors.append({"sid": s["sid"], "err": f"plc:{str(e)[:100]}"})
                if (include_targeting and not placement_only) or targeting_only:
                    try:
                        kw_r, tgt_r = await fetcher.fetch_targeting_day(s["sid"], s["name"], ds)
                        db.upsert_targeting_kw_rows(kw_r)
                        db.upsert_targeting_tgt_rows(tgt_r)
                        kw_total += len(kw_r); tgt_total += len(tgt_r)
                    except Exception as e:
                        log.exception(f"backfill sid={s['sid']} date={ds} tgt fail")
                        day_errors.append({"sid": s["sid"], "err": f"tgt:{str(e)[:100]}"})
                await asyncio.sleep(0.5)
            log.info(f"backfill {ds} st={day_total} plc={plc_total} kw={kw_total} tgt={tgt_total} errors={len(day_errors)}")
            await asyncio.sleep(sleep_between_days)
            cur += timedelta(days=1)
        await feishu.alert_frankie(f"backfill {start_date}..{end_date} done")
    except Exception as e:
        log.exception("backfill bg fatal")
        await feishu.alert_frankie(f"backfill {start_date}..{end_date} FATAL: {str(e)[:300]}")


@app.post("/backfill")
async def backfill(req: BackfillRequest, background: BackgroundTasks, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    if date.fromisoformat(req.start_date) > date.fromisoformat(req.end_date):
        raise HTTPException(status_code=400, detail="start_date > end_date")
    background.add_task(_backfill_run, req.start_date, req.end_date, req.sids,
                       req.sleep_between_days, req.include_placement, req.include_targeting,
                       req.placement_only, req.targeting_only)
    return {"accepted": True, "start": req.start_date, "end": req.end_date,
            "sids": req.sids or "all-active",
            "include_placement": req.include_placement, "include_targeting": req.include_targeting,
            "placement_only": req.placement_only, "targeting_only": req.targeting_only}


class QueryRequest(BaseModel):
    end_date: Optional[str] = None    # default = T-8
    days_total: int = 60              # total window for query
    t14: int = 14
    t30: int = 30
    t60: int = 60
    offset_days: int = 8              # avoid attribution window


@app.post("/query")
async def query_windows(req: QueryRequest, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    end = date.fromisoformat(req.end_date) if req.end_date else (date.today() - timedelta(days=req.offset_days))
    start = end - timedelta(days=req.days_total - 1)
    t_14 = end - timedelta(days=req.t14 - 1)
    t_30 = end - timedelta(days=req.t30 - 1)
    t_60 = end - timedelta(days=req.t60 - 1)
    rows = db.aggregate_windows(start, end, t_14, t_30, t_60)
    # cast numerics to float for JSON
    for r in rows:
        for k, v in list(r.items()):
            if hasattr(v, "is_finite"):
                r[k] = float(v)
            elif isinstance(v, date):
                r[k] = v.isoformat()
    return {
        "end_date": end.isoformat(),
        "start_date": start.isoformat(),
        "t_14": t_14.isoformat(), "t_30": t_30.isoformat(), "t_60": t_60.isoformat(),
        "rows": rows, "count": len(rows),
    }


class WeeklyDataRequest(BaseModel):
    end_date: Optional[str] = None    # YYYY-MM-DD override; otherwise computed as T - offset_days
    t14: int = 14
    t30: int = 30
    t60: int = 60
    offset_days: Optional[int] = None  # for manual testing; production = always 8 (v2 sub-task ②)


PRODUCTION_OFFSET_DAYS = 8  # T-8 = avoid 7-day attribution window (Q2 decision, locked)


@app.post("/weekly-data")
async def weekly_data(req: WeeklyDataRequest = None, authorization: Optional[str] = Header(None)):
    """Aggregated payload for v2 weekly report (n8n N2 hits this).

    Production rule: end_date = T-8 (avoid attribution window). Body is optional;
    n8n production call can be parameterless. Manual override via end_date or offset_days for testing.

    Returns: report_date, windows{}, owner_results{owner:{store:{boost/scale/negate/warn/...}}}, stats{}.
    Side effect: persists this week's recs into search_term_recommendation for next-week diff.
    """
    _auth(authorization)
    if req is None:
        req = WeeklyDataRequest()
    end = date.fromisoformat(req.end_date) if req.end_date else None
    offset = req.offset_days if req.offset_days is not None else PRODUCTION_OFFSET_DAYS
    return await weekly.build_weekly_data_async(end_date=end, t14=req.t14, t30=req.t30, t60=req.t60,
                                                offset_days=offset)


@app.get("/coverage")
async def coverage(start: str, end: str, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    rows = db.coverage_check(date.fromisoformat(start), date.fromisoformat(end))
    for r in rows:
        if isinstance(r.get("report_date"), date):
            r["report_date"] = r["report_date"].isoformat()
    return {"start": start, "end": end, "rows": rows}


@app.post("/snapshot-negwords")
async def snapshot_negwords(authorization: Optional[str] = Header(None)):
    """Capture current state of negative keywords across all active sellers.
    Used by reporting layer to diff against last-week recommendations."""
    _auth(authorization)
    from . import lingxing
    sellers = await fetcher.fetch_active_sellers()
    today = date.today().isoformat()
    total = 0
    with db.conn() as c:
        cur = c.cursor()
        cur.execute("DELETE FROM amazon_ads.neg_keyword_snapshot WHERE snapshot_date = %s", (today,))
        for s in sellers:
            try:
                rows = await lingxing.query_neg_words(s["sid"])
            except Exception as e:
                log.warning(f"negwords sid={s['sid']} fail: {e}")
                continue
            for r in rows:
                neg = (r.get("keyword_text") or r.get("neg_text") or "").strip()
                if not neg:
                    continue
                cur.execute(
                    "INSERT INTO amazon_ads.neg_keyword_snapshot (snapshot_date,sid,campaign_id,neg_text,match_type) "
                    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (today, s["sid"], r.get("campaign_id"), neg, r.get("match_type") or "")
                )
                total += 1
            await asyncio.sleep(0.5)
    return {"snapshot_date": today, "sellers": len(sellers), "rows": total}
