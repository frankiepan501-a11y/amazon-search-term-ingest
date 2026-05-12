import asyncio
import logging
import os
from datetime import date, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from . import db, fetcher, feishu, config

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


@app.post("/daily")
async def daily(req: DailyRequest, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    target_date = req.date or (date.today() - timedelta(days=2)).isoformat()
    try:
        sellers = await fetcher.fetch_active_sellers()
        if req.sids:
            allow = {int(s) for s in req.sids}
            sellers = [s for s in sellers if s["sid"] in allow]
        log.info(f"daily start date={target_date} sellers={len(sellers)}")
        total_rows = 0
        per_seller = []
        errors = []
        for s in sellers:
            try:
                rows = await fetcher.fetch_day_rows(s["sid"], s["name"], target_date)
                n = db.upsert_daily_rows(rows)
                total_rows += len(rows)
                per_seller.append({"sid": s["sid"], "name": s["name"], "rows": len(rows)})
            except Exception as e:
                log.exception(f"sid={s['sid']} fail")
                errors.append({"sid": s["sid"], "name": s["name"], "err": str(e)[:200]})
            await asyncio.sleep(0.5)
        result = {"date": target_date, "sellers": len(sellers), "total_rows": total_rows,
                  "per_seller": per_seller, "errors": errors}
        if errors and len(errors) >= max(1, len(sellers) // 3):
            _state["consecutive_failures"] += 1
            msg = f"daily {target_date} {len(errors)}/{len(sellers)} 店失败 (连续 {_state['consecutive_failures']} 次): {errors[:3]}"
            await feishu.alert_frankie(msg)
            if _state["consecutive_failures"] >= 3:
                await feishu.alert_group(msg)
        else:
            _state["consecutive_failures"] = 0
        return result
    except Exception as e:
        log.exception("daily fatal")
        _state["consecutive_failures"] += 1
        await feishu.alert_frankie(f"daily {target_date} FATAL: {str(e)[:300]} (连续 {_state['consecutive_failures']} 次)")
        if _state["consecutive_failures"] >= 3:
            await feishu.alert_group(f"daily {target_date} FATAL 连续 3 次以上: {str(e)[:300]}")
        raise HTTPException(status_code=500, detail=str(e))


class BackfillRequest(BaseModel):
    start_date: str  # YYYY-MM-DD inclusive
    end_date: str    # YYYY-MM-DD inclusive
    sids: Optional[list] = None
    sleep_between_days: float = 2.0


@app.post("/backfill")
async def backfill(req: BackfillRequest, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    start = date.fromisoformat(req.start_date)
    end = date.fromisoformat(req.end_date)
    if start > end:
        raise HTTPException(status_code=400, detail="start_date > end_date")
    sellers = await fetcher.fetch_active_sellers()
    if req.sids:
        allow = {int(s) for s in req.sids}
        sellers = [s for s in sellers if s["sid"] in allow]
    log.info(f"backfill {start}..{end} sellers={len(sellers)}")
    summary = []
    cur = start
    while cur <= end:
        ds = cur.isoformat()
        day_total = 0
        day_errors = []
        for s in sellers:
            try:
                rows = await fetcher.fetch_day_rows(s["sid"], s["name"], ds)
                db.upsert_daily_rows(rows)
                day_total += len(rows)
            except Exception as e:
                log.exception(f"backfill sid={s['sid']} date={ds} fail")
                day_errors.append({"sid": s["sid"], "err": str(e)[:200]})
            await asyncio.sleep(0.5)
        summary.append({"date": ds, "rows": day_total, "errors": day_errors})
        log.info(f"backfill {ds} rows={day_total} errors={len(day_errors)}")
        await asyncio.sleep(req.sleep_between_days)
        cur += timedelta(days=1)
    return {"start": req.start_date, "end": req.end_date, "summary": summary}


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
