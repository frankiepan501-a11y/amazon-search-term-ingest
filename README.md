# Amazon Search Term Daily Ingest

Webhook service for n8n workflow `搜索词分析报告 v2 BETA`. Pulls daily Lingxing ad-report data into Postgres so the weekly report can aggregate 14d/30d/60d windows.

## Endpoints

| Method | Path | Purpose | Auth |
|---|---|---|---|
| GET  | `/`        | health check | none |
| POST | `/init`    | create schema + tables + indexes (idempotent) | Bearer |
| POST | `/daily`   | ingest one day (default T-2) for all active sellers | Bearer |
| POST | `/backfill`| ingest a date range, day-by-day | Bearer |
| POST | `/query`   | aggregate 14d/30d/60d for weekly report | Bearer |
| POST | `/snapshot-negwords` | snapshot current neg-keyword list (diff source for §3.6) | Bearer |
| GET  | `/coverage`| per-(sid,date) row count — find missing days | Bearer |

## Env vars

- `LINGXING_APP_ID` / `LINGXING_APP_SECRET`
- `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_ALERT_USER_OPENID` / `FEISHU_ALERT_GROUP_CHATID`
- `POSTGRESQL_HOST` / `POSTGRESQL_PORT` / `POSTGRESQL_USER` / `POSTGRESQL_PASSWORD` / `POSTGRESQL_DATABASE` — auto-injected by Zeabur when same project
- `API_BEARER_TOKEN` — required header `Authorization: Bearer <token>` on every non-`/` request

## Failure handling (Q6)

- single day failure or partial (1/3+ sellers fail) → 飞书私聊 Frankie
- 3 consecutive daily-cron failures → also 群告警
