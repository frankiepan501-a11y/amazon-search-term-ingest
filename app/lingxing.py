import base64
import hashlib
import time
import urllib.parse
import asyncio
import httpx
from Crypto.Cipher import AES
from . import config

BASE = "https://openapi.lingxing.com"
_token_cache = {"token": None, "expires": 0}


def _sign(params: dict) -> str:
    items = sorted((k, v) for k, v in params.items() if v not in ("", None))
    raw = "&".join(f"{k}={v}" for k, v in items)
    md5 = hashlib.md5(raw.encode()).hexdigest().upper()
    key = config.LINGXING_APP_ID.encode()
    key = key[:16] if len(key) >= 16 else key.ljust(16, b"\0")
    cipher = AES.new(key, AES.MODE_ECB)
    pad = 16 - (len(md5) % 16)
    padded = md5 + chr(pad) * pad
    return base64.b64encode(cipher.encrypt(padded.encode())).decode()


async def get_token() -> str:
    now = time.time()
    if _token_cache["token"] and _token_cache["expires"] - 60 > now:
        return _token_cache["token"]
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{BASE}/api/auth-server/oauth/access-token",
            data={"appId": config.LINGXING_APP_ID, "appSecret": config.LINGXING_APP_SECRET},
        )
        d = r.json()
        tok = d["data"]["access_token"]
        expires = now + int(d["data"].get("expires_in", 7200))
        _token_cache["token"] = tok
        _token_cache["expires"] = expires
        return tok


async def call(path: str, params: dict, is_newad: bool = False, retries: int = 5) -> dict:
    token = await get_token()
    for attempt in range(retries):
        ts = str(int(time.time()))
        common = {"access_token": token, "app_key": config.LINGXING_APP_ID, "timestamp": ts}
        sp = {**common, **{k: str(v) for k, v in params.items()}}
        sign = urllib.parse.quote(_sign(sp))
        qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in common.items()) + f"&sign={sign}"
        headers = {"Content-Type": "application/json"}
        if is_newad:
            headers["X-API-VERSION"] = "2"
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(f"{BASE}{path}?{qs}", json=params, headers=headers)
            try:
                d = r.json()
            except Exception:
                d = {"code": -1, "raw": r.text[:300]}
        code = str(d.get("code"))
        if code == "3001002":
            await asyncio.sleep(8 + attempt * 2)
            continue
        return d
    return {"code": -1, "msg": "max retries"}


async def call_all(path: str, params: dict, page_size: int = 100, is_newad: bool = False) -> list:
    out = []
    offset = 0
    while True:
        p = {**params, "offset": offset, "length": page_size}
        d = await call(path, p, is_newad=is_newad)
        if d.get("code") != 0:
            break
        data = d.get("data") or []
        out.extend(data)
        total = d.get("total") or 0
        if offset + page_size >= total or not data:
            break
        offset += page_size
        await asyncio.sleep(0.4)
    return out


async def list_sellers() -> list:
    return await call_all("/erp/sc/data/seller/lists", {}, page_size=200, is_newad=False)


async def query_word_keyword(sid: int, report_date: str) -> list:
    return await call_all(
        "/pb/openapi/newad/queryWordReports",
        {"sid": sid, "report_date": report_date, "target_type": "keyword", "show_detail": 0},
        page_size=100, is_newad=True,
    )


async def query_word_target(sid: int, report_date: str) -> list:
    return await call_all(
        "/pb/openapi/newad/queryWordReports",
        {"sid": sid, "report_date": report_date, "target_type": "target", "show_detail": 0},
        page_size=100, is_newad=True,
    )


async def hsa_query_word(sid: int, report_date: str) -> list:
    return await call_all(
        "/pb/openapi/newad/hsaQueryWordReports",
        {"sid": sid, "report_date": report_date, "target_type": "keyword", "show_detail": 0},
        page_size=100, is_newad=True,
    )


async def sp_product_ads(sid: int) -> list:
    return await call_all(
        "/pb/openapi/newad/spProductAds",
        {"sid": sid},
        page_size=100, is_newad=True,
    )


async def sp_keyword_reports(sid: int, report_date: str) -> list:
    return await call_all(
        "/pb/openapi/newad/spKeywordReports",
        {"sid": sid, "report_date": report_date, "show_detail": 0},
        page_size=100, is_newad=True,
    )


async def listings(sid: int) -> list:
    return await call_all(
        "/erp/sc/data/mws/listing",
        {"sid": sid},
        page_size=100, is_newad=False,
    )


async def query_neg_words(sid: int) -> list:
    """Active negative keywords (current state, not historical)."""
    return await call_all(
        "/pb/openapi/newad/queryNegWords",
        {"sid": sid},
        page_size=100, is_newad=True,
    )
