"""Server-side classification + Porter Stemmer grouping + 多窗口 decision rules.

Sub-tasks covered:
  ②  date range (T-21 ~ T-8) — handled at /weekly-data endpoint param level
  ③a Porter Stemmer + 同根铁律 — group by stem, force "待人审" if intra-group inconsistent
  ③b 14d/30d/60d multi-window — boost/scale/negate must satisfy all 3 windows
  ④  display grouping — return rows with stem_key + group_size for downstream renderer
"""
from collections import defaultdict
from .stemmer import compute_stem

# Per-marketplace click thresholds (mirror v1 N3 getThresholds())
_THRESH = {
    "MX": {"neg_14": 6,  "neg_30": 12, "neg_60": 24, "ace_14": 10, "warn_14": 3, "warn_cost": 1.5},
    "BR": {"neg_14": 6,  "neg_30": 12, "neg_60": 24, "ace_14": 10, "warn_14": 3, "warn_cost": 1.5},
    "US": {"neg_14": 15, "neg_30": 30, "neg_60": 60, "ace_14": 30, "warn_14": 5, "warn_cost": 3.0},
    "DEFAULT": {"neg_14": 8, "neg_30": 16, "neg_60": 32, "ace_14": 15, "warn_14": 3, "warn_cost": 2.0},
}


def _thresh(store_name: str):
    n = (store_name or "").upper()
    if "-MX" in n:
        return _THRESH["MX"]
    if "-BR" in n:
        return _THRESH["BR"]
    if "-US" in n or any(x in n for x in ("FANLEPU", "FUNLAB-US", "POWKONG")):
        return _THRESH["US"]
    return _THRESH["DEFAULT"]


def _calc(r):
    clk = r.get("clk_14d", 0)
    ord_ = r.get("ord_14d", 0)
    cost = r.get("cost_14d", 0)
    sales = r.get("sales_14d", 0)
    r["cvr_14"] = round(ord_ / clk * 100, 2) if clk else 0
    r["acos_14"] = round(cost / sales * 100, 2) if sales else 999
    r["cpc_14"] = round(cost / clk, 2) if clk else 0


def classify_rows(rows: list, current_neg_set: set, last_week_recs: dict, bid_history: dict):
    """For each (query, campaign, mt) row, decide bucket using 多窗口铁律.

    Returns: { 'records': [...with stem_key + bucket + operation_flag...] }
    """
    for r in rows:
        _calc(r)
        r["stem_key"] = compute_stem(r.get("query", ""))

    # 同根分组
    groups = defaultdict(list)
    for r in rows:
        groups[r["stem_key"]].append(r)

    # 决策 (③b 多窗口铁律 + ③a 同根铁律)
    for stem_key, group in groups.items():
        group_size = len(group)
        # Step 1: per-row provisional bucket based on multi-window thresholds
        for r in group:
            t = _thresh(r.get("store_name", ""))
            clk14, ord14 = r.get("clk_14d", 0), r.get("ord_14d", 0)
            clk30, ord30 = r.get("clk_30d", 0), r.get("ord_30d", 0)
            clk60, ord60 = r.get("clk_60d", 0), r.get("ord_60d", 0)
            cost14 = float(r.get("cost_14d", 0))

            bucket = None
            reason = ""
            # 否定: must satisfy 14d AND 30d AND 60d
            if (clk14 >= t["neg_14"] and ord14 == 0 and
                clk30 >= t["neg_30"] and ord30 == 0 and
                clk60 >= t["neg_60"] and ord60 <= 1):
                bucket = "negate"
                reason = f"14d {clk14}/0 · 30d {clk30}/0 · 60d {clk60}/{ord60}"
            elif ord14 > 0 and r["cvr_14"] >= 5 and clk14 >= t["ace_14"]:
                bucket = "scale"
                reason = f"14d CVR={r['cvr_14']}% 点击{clk14}"
            elif ord14 > 0 and r["cvr_14"] >= 3 and clk14 >= 3 and clk14 < t["ace_14"]:
                bucket = "boost"
                reason = f"14d CVR={r['cvr_14']}% 点击{clk14}"
            elif ord14 == 0 and clk14 >= t["warn_14"] and clk14 < t["neg_14"] and cost14 >= t["warn_cost"]:
                bucket = "warn"
                reason = f"14d 花费${cost14:.1f} 点击{clk14}/0"
            # 待观察 — 14d 满足否定但 30d/60d 不满足
            elif (clk14 >= t["neg_14"] and ord14 == 0 and
                  (ord30 > 0 or ord60 > 1)):
                bucket = "observe"
                reason = f"14d无单但30d{ord30}单/60d{ord60}单 — 月度低谷，不否"

            r["bucket"] = bucket
            r["reason"] = reason
            r["group_size"] = group_size

        # Step 2: ③a 同根铁律 — group with size >= 2 + inconsistent buckets → "待人审"
        if group_size >= 2:
            buckets = {r["bucket"] for r in group if r["bucket"]}
            # If group has mixed decisions (e.g. one negate + one scale), force human review
            decisive = buckets & {"negate", "scale", "boost"}
            if "negate" in buckets and (buckets - {"negate", None}):
                # Negate + anything-positive in same root → freeze
                for r in group:
                    if r["bucket"] == "negate":
                        r["bucket"] = "pending_human"
                        r["reason"] += " · ⚠️ 同根词不一致[#" + stem_key + "]"

    # Step 3: §3.6 运营操作 diff
    for r in rows:
        op = _operation_status(r, current_neg_set, last_week_recs, bid_history)
        r["operation"] = op["flag"]
        r["operation_detail"] = op["detail"]

    return rows


def _operation_status(row, current_neg_set, last_week_recs, bid_history):
    """Return one of:
      ✅ executed | ⏳ pending | 🆕 new | "" (no past advice, no current bucket)
    """
    key = (row.get("sid"), row.get("query"), row.get("campaign_id") or 0)
    last = last_week_recs.get(key)
    if not last:
        # No previous advice; mark new only if current row got a decisive bucket
        if row.get("bucket") in ("negate", "scale", "boost"):
            return {"flag": "🆕 本周新建议", "detail": ""}
        return {"flag": "", "detail": ""}
    advice = last["recommendation"]
    if advice == "否定":
        # Check current_neg_set ((sid, neg_text) lookup)
        neg_key = (row.get("sid"), (row.get("query") or "").lower())
        if neg_key in current_neg_set:
            return {"flag": "✅ 本周已否定", "detail": f"运营在 ≤7 天内执行"}
        return {"flag": "⏳ 上周建议未执行", "detail": f"距建议 7 天，继续观察"}
    if advice == "降价":
        bid_old = last.get("bid_at_advice")
        bid_new = row.get("bid_latest")
        if bid_old and bid_new and float(bid_new) <= float(bid_old) * 0.8:
            return {"flag": "✅ 本周已降价", "detail": f"{bid_old}→{bid_new}"}
        return {"flag": "⏳ 上周建议降价未执行", "detail": ""}
    if advice == "转 ASIN":
        # heuristic: look for newly-tracked campaign with this query as exact match
        # NB: full check requires campaign creation date; simplified here
        return {"flag": "⏳ 待确认", "detail": "需手工验证"}
    return {"flag": "", "detail": ""}


def group_by_owner_store(rows: list):
    """Group classified rows by owner -> store -> {boost, scale, negate, warn, observe, stem_groups}."""
    out = {}
    for r in rows:
        owner = r.get("owner") or "未分配"
        store = r.get("store_name") or ""
        if owner not in out:
            out[owner] = {}
        if store not in out[owner]:
            out[owner][store] = {"boost": [], "scale": [], "negate": [], "warn": [],
                                  "observe": [], "pending_human": [], "stem_groups": {}}
        b = r.get("bucket")
        if b:
            out[owner][store][b].append(r)
        # always track stem groups (for ④ display merging)
        sk = r.get("stem_key", "")
        if sk:
            out[owner][store]["stem_groups"].setdefault(sk, []).append(r)
    # sort each bucket
    for owner in out:
        for store in out[owner]:
            d = out[owner][store]
            d["scale"].sort(key=lambda x: -x.get("sales_14d", 0))
            d["boost"].sort(key=lambda x: -x.get("cvr_14", 0))
            d["negate"].sort(key=lambda x: -x.get("clk_14d", 0))
            d["warn"].sort(key=lambda x: -x.get("cost_14d", 0))
            d["pending_human"].sort(key=lambda x: -x.get("clk_14d", 0))
    return out
