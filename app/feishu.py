import time
import httpx
import json
from . import config

_token_cache = {"token": None, "expires": 0}


async def get_token() -> str:
    now = time.time()
    if _token_cache["token"] and _token_cache["expires"] - 60 > now:
        return _token_cache["token"]
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": config.FEISHU_APP_ID, "app_secret": config.FEISHU_APP_SECRET},
        )
        d = r.json()
        _token_cache["token"] = d["tenant_access_token"]
        _token_cache["expires"] = now + 7000
        return _token_cache["token"]


async def send_text(receive_id: str, receive_id_type: str, text: str):
    token = await get_token()
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"receive_id": receive_id, "msg_type": "text",
                  "content": json.dumps({"text": text}, ensure_ascii=False)},
        )
        return r.json()


async def alert_frankie(text: str):
    if not config.FEISHU_ALERT_USER_OPENID:
        return
    return await send_text(config.FEISHU_ALERT_USER_OPENID, "open_id", f"[搜索词 v2 daily cron] {text}")


async def alert_group(text: str):
    if not config.FEISHU_ALERT_GROUP_CHATID:
        return
    return await send_text(config.FEISHU_ALERT_GROUP_CHATID, "chat_id", f"[搜索词 v2 daily cron] {text}")
