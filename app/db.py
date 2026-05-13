import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from . import config

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS amazon_ads;"

DDL_DAILY = """
CREATE TABLE IF NOT EXISTS amazon_ads.search_term_daily (
  sid           INT          NOT NULL,
  store_name    TEXT,
  report_date   DATE         NOT NULL,
  query         TEXT         NOT NULL,
  campaign_id   BIGINT       NOT NULL DEFAULT 0,
  campaign_name TEXT,
  target_text   TEXT,
  match_type    TEXT         NOT NULL DEFAULT '',
  asin          TEXT,
  owner         TEXT,
  impressions   INT          DEFAULT 0,
  clicks        INT          DEFAULT 0,
  cost          NUMERIC(12,4) DEFAULT 0,
  orders        INT          DEFAULT 0,
  same_orders   INT          DEFAULT 0,
  sales         NUMERIC(12,4) DEFAULT 0,
  same_sales    NUMERIC(12,4) DEFAULT 0,
  units         INT          DEFAULT 0,
  bid           NUMERIC(8,4),
  source_api    TEXT,
  ingested_at   TIMESTAMP    DEFAULT NOW(),
  PRIMARY KEY (sid, report_date, query, campaign_id, match_type)
);
"""

DDL_INDEX_DAILY = [
    "CREATE INDEX IF NOT EXISTS idx_std_lookup ON amazon_ads.search_term_daily (sid, report_date DESC, query);",
    "CREATE INDEX IF NOT EXISTS idx_std_owner  ON amazon_ads.search_term_daily (owner, report_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_std_query  ON amazon_ads.search_term_daily (query);",
]

DDL_REC = """
CREATE TABLE IF NOT EXISTS amazon_ads.search_term_recommendation (
  report_date    DATE NOT NULL,
  sid            INT  NOT NULL,
  store_name     TEXT,
  owner          TEXT,
  query          TEXT NOT NULL,
  campaign_id    BIGINT NOT NULL DEFAULT 0,
  match_type     TEXT NOT NULL DEFAULT '',
  recommendation TEXT NOT NULL,
  reason         TEXT,
  status         TEXT DEFAULT 'pending',
  executed_at    TIMESTAMP,
  exec_detail    TEXT,
  bid_at_advice  NUMERIC(8,4),
  PRIMARY KEY (report_date, sid, query, campaign_id, match_type, recommendation)
);
"""

DDL_INDEX_REC = [
    "CREATE INDEX IF NOT EXISTS idx_rec_lookup ON amazon_ads.search_term_recommendation (sid, query, report_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_rec_status ON amazon_ads.search_term_recommendation (status, report_date DESC);",
]

DDL_NEG_SNAPSHOT = """
CREATE TABLE IF NOT EXISTS amazon_ads.neg_keyword_snapshot (
  snapshot_date DATE NOT NULL,
  sid           INT  NOT NULL,
  campaign_id   BIGINT,
  neg_text      TEXT NOT NULL,
  match_type    TEXT,
  PRIMARY KEY (snapshot_date, sid, campaign_id, neg_text, match_type)
);
"""

DDL_PLACEMENT = """
CREATE TABLE IF NOT EXISTS amazon_ads.placement_daily (
  sid            INT          NOT NULL,
  store_name     TEXT,
  report_date    DATE         NOT NULL,
  campaign_id    BIGINT       NOT NULL DEFAULT 0,
  placement_type TEXT         NOT NULL,
  owner          TEXT,
  impressions    INT          DEFAULT 0,
  clicks         INT          DEFAULT 0,
  cost           NUMERIC(12,4) DEFAULT 0,
  orders         INT          DEFAULT 0,
  sales          NUMERIC(12,4) DEFAULT 0,
  ingested_at    TIMESTAMP    DEFAULT NOW(),
  PRIMARY KEY (sid, report_date, campaign_id, placement_type)
);
"""

DDL_INDEX_PLACEMENT = [
    "CREATE INDEX IF NOT EXISTS idx_plc_lookup ON amazon_ads.placement_daily (sid, report_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_plc_owner  ON amazon_ads.placement_daily (owner, report_date DESC);",
]


@contextmanager
def conn():
    c = psycopg.connect(config.DSN, autocommit=False, row_factory=dict_row)
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def init_schema():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(DDL_SCHEMA)
            cur.execute(DDL_DAILY)
            for sql in DDL_INDEX_DAILY:
                cur.execute(sql)
            cur.execute(DDL_REC)
            for sql in DDL_INDEX_REC:
                cur.execute(sql)
            cur.execute(DDL_NEG_SNAPSHOT)
            cur.execute(DDL_PLACEMENT)
            for sql in DDL_INDEX_PLACEMENT:
                cur.execute(sql)
    return {"ok": True, "tables": ["search_term_daily", "search_term_recommendation",
                                    "neg_keyword_snapshot", "placement_daily"]}


UPSERT_DAILY = """
INSERT INTO amazon_ads.search_term_daily
(sid, store_name, report_date, query, campaign_id, campaign_name, target_text, match_type,
 asin, owner, impressions, clicks, cost, orders, same_orders, sales, same_sales, units, bid, source_api)
VALUES (%(sid)s,%(store_name)s,%(report_date)s,%(query)s,%(campaign_id)s,%(campaign_name)s,%(target_text)s,%(match_type)s,
        %(asin)s,%(owner)s,%(impressions)s,%(clicks)s,%(cost)s,%(orders)s,%(same_orders)s,%(sales)s,%(same_sales)s,%(units)s,%(bid)s,%(source_api)s)
ON CONFLICT (sid, report_date, query, campaign_id, match_type) DO UPDATE SET
  store_name=EXCLUDED.store_name,
  campaign_name=EXCLUDED.campaign_name,
  target_text=EXCLUDED.target_text,
  asin=EXCLUDED.asin,
  owner=EXCLUDED.owner,
  impressions=EXCLUDED.impressions,
  clicks=EXCLUDED.clicks,
  cost=EXCLUDED.cost,
  orders=EXCLUDED.orders,
  same_orders=EXCLUDED.same_orders,
  sales=EXCLUDED.sales,
  same_sales=EXCLUDED.same_sales,
  units=EXCLUDED.units,
  bid=COALESCE(EXCLUDED.bid, amazon_ads.search_term_daily.bid),
  source_api=EXCLUDED.source_api,
  ingested_at=NOW();
"""


def upsert_daily_rows(rows):
    if not rows:
        return 0
    with conn() as c:
        with c.cursor() as cur:
            cur.executemany(UPSERT_DAILY, rows)
            return cur.rowcount


def aggregate_windows(start_date, end_date, t_14, t_30, t_60):
    """Aggregate metrics across 14d / 30d / 60d windows for all (query, campaign, match_type) groups."""
    sql = """
    SELECT query, campaign_id, campaign_name, owner, store_name, sid, match_type,
      SUM(CASE WHEN report_date >= %(t14)s THEN impressions ELSE 0 END) AS imp_14d,
      SUM(CASE WHEN report_date >= %(t14)s THEN clicks      ELSE 0 END) AS clk_14d,
      SUM(CASE WHEN report_date >= %(t14)s THEN cost        ELSE 0 END) AS cost_14d,
      SUM(CASE WHEN report_date >= %(t14)s THEN orders      ELSE 0 END) AS ord_14d,
      SUM(CASE WHEN report_date >= %(t14)s THEN sales       ELSE 0 END) AS sales_14d,
      SUM(CASE WHEN report_date >= %(t14)s THEN units       ELSE 0 END) AS units_14d,
      SUM(CASE WHEN report_date >= %(t30)s THEN clicks      ELSE 0 END) AS clk_30d,
      SUM(CASE WHEN report_date >= %(t30)s THEN orders      ELSE 0 END) AS ord_30d,
      SUM(CASE WHEN report_date >= %(t30)s THEN cost        ELSE 0 END) AS cost_30d,
      SUM(CASE WHEN report_date >= %(t30)s THEN sales       ELSE 0 END) AS sales_30d,
      SUM(CASE WHEN report_date >= %(t60)s THEN clicks      ELSE 0 END) AS clk_60d,
      SUM(CASE WHEN report_date >= %(t60)s THEN orders      ELSE 0 END) AS ord_60d,
      SUM(CASE WHEN report_date >= %(t60)s THEN cost        ELSE 0 END) AS cost_60d,
      SUM(CASE WHEN report_date >= %(t60)s THEN sales       ELSE 0 END) AS sales_60d,
      MAX(bid) AS bid_latest
    FROM amazon_ads.search_term_daily
    WHERE report_date BETWEEN %(start)s AND %(end)s
    GROUP BY query, campaign_id, campaign_name, owner, store_name, sid, match_type;
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"start": start_date, "end": end_date, "t14": t_14, "t30": t_30, "t60": t_60})
            return cur.fetchall()


UPSERT_PLACEMENT = """
INSERT INTO amazon_ads.placement_daily
(sid, store_name, report_date, campaign_id, placement_type, owner,
 impressions, clicks, cost, orders, sales)
VALUES (%(sid)s,%(store_name)s,%(report_date)s,%(campaign_id)s,%(placement_type)s,%(owner)s,
        %(impressions)s,%(clicks)s,%(cost)s,%(orders)s,%(sales)s)
ON CONFLICT (sid, report_date, campaign_id, placement_type) DO UPDATE SET
  store_name=EXCLUDED.store_name,
  owner=EXCLUDED.owner,
  impressions=EXCLUDED.impressions,
  clicks=EXCLUDED.clicks,
  cost=EXCLUDED.cost,
  orders=EXCLUDED.orders,
  sales=EXCLUDED.sales,
  ingested_at=NOW();
"""


def upsert_placement_rows(rows):
    if not rows:
        return 0
    with conn() as c:
        with c.cursor() as cur:
            cur.executemany(UPSERT_PLACEMENT, rows)
            return cur.rowcount


def aggregate_placement_14d(start_date, end_date):
    """Aggregate placement metrics across the 14-day window grouped by (owner, store, placement_type).
    14d-only is sufficient for placement (no multi-window rule); follows v1 N5 expectation."""
    sql = """
    SELECT owner, store_name, sid, placement_type,
      SUM(impressions) AS impressions,
      SUM(clicks)      AS clicks,
      SUM(cost)        AS cost,
      SUM(orders)      AS orders,
      SUM(sales)       AS sales
    FROM amazon_ads.placement_daily
    WHERE report_date BETWEEN %(start)s AND %(end)s
    GROUP BY owner, store_name, sid, placement_type;
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"start": start_date, "end": end_date})
            return cur.fetchall()


def coverage_check(start_date, end_date):
    """Return per-(sid, date) row count. Used to detect missing days."""
    sql = """
    SELECT sid, report_date, COUNT(*) AS rows
    FROM amazon_ads.search_term_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY sid, report_date
    ORDER BY sid, report_date;
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
            return cur.fetchall()
