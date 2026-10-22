"""诊断脚本：探测 B 站接口当前是否被风控。

只发少量请求，用于判断 -352 是"签名算错"还是"真被风控"。

用法：
    python tools/probe_bili.py [UID]
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import aiohttp  # noqa: E402

from astrbot_plugin_bili_subscription.bili_api import (  # noqa: E402
    API_ORIGIN,
    BiliApiError,
    BiliSubscriptionClient,
    _sign_params,
    fetch_bili_ticket,
)
from astrbot_plugin_bili_subscription.bili_player_bridge import (  # noqa: E402
    has_bilibili_login,
    read_bilibili_cookies,
)

DATA_DIR = Path(r"D:\bot\AstrBotLauncher-0.3.0\AstrBotLauncher-0.3.0\AstrBot\data")
PLAYER_DIR = DATA_DIR / "plugin_data" / "astrbot_plugin_bili_player"


async def probe_raw(session: aiohttp.ClientSession, uid: str) -> None:
    """裸请求 NAV，打印真实返回，确认网络/风控状态。"""
    print("--- ① 裸请求 x/web-interface/nav ---")
    try:
        async with session.get(
            f"{API_ORIGIN}/x/web-interface/nav",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Referer": "https://www.bilibili.com/",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            text = await resp.text()
            print(f"    HTTP {resp.status}")
            print(f"    body[:300] = {text[:300]}")
    except Exception as exc:
        print(f"    请求异常：{exc!r}")


async def probe_client(session: aiohttp.ClientSession, uid: str) -> None:
    print("\n--- ② 走插件客户端抓 1 个 UID ---")
    cookies = read_bilibili_cookies(PLAYER_DIR)
    print(f"    登录 Cookie 数量：{len(cookies)}")
    important = [
        k for k in cookies
        if k in ("SESSDATA", "bili_jct", "DedeUserID", "buvid3", "buvid4",
                 "b_nut", "_uuid", "bili_ticket")
    ]
    print(f"    关键 Cookie：{sorted(important)}")

    client = BiliSubscriptionClient(
        session,
        credentials_getter=lambda: cookies,
        min_request_gap=2.0,
    )
    for label, coro in (
        ("fetch_videos(limit=1)", lambda: client.fetch_videos(uid, limit=1)),
        ("fetch_dynamics(limit=1)", lambda: client.fetch_dynamics(uid, limit=1)),
        ("fetch_articles(limit=1)", lambda: client.fetch_articles(uid, limit=1)),
    ):
        started = time.monotonic()
        try:
            result = await coro()
            elapsed = time.monotonic() - started
            print(f"    {label}: OK，{len(result)} 条，用时 {elapsed:.1f}s")
            if result:
                first = result[0]
                ident = getattr(first, "bvid", None) or getattr(
                    first, "dynamic_id", None
                ) or getattr(first, "article_id", None)
                print(f"        首条：{ident}")
        except BiliApiError as exc:
            elapsed = time.monotonic() - started
            print(f"    {label}: 失败 code={exc.code}，用时 {elapsed:.1f}s")
            print(f"        {exc}")
    print(f"    最终节流间隔：{client.current_min_gap():.1f}s")


async def main() -> int:
    uid = sys.argv[1] if len(sys.argv) > 1 else "161775300"
    print(f"目标 UID：{uid}")
    print(f"登录检测 has_bilibili_login：{has_bilibili_login(PLAYER_DIR)}")

    # 顺带验证签名算法本身是否自洽（固定时间戳可复现）
    sample = _sign_params({"mid": uid, "ps": 1, "pn": 1}, "abc123", timestamp=1700000000)
    print(f"签名自检（固定 wts）：w_rid={sample.get('w_rid')}")

    connector = aiohttp.TCPConnector(limit=4, limit_per_host=2)
    async with aiohttp.ClientSession(connector=connector) as session:
        ticket = await fetch_bili_ticket(session)
        print(f"bili_ticket 获取：{'成功 ' + str(len(ticket.get('bili_ticket', ''))) + ' 字符' if ticket else '失败'}")
        await probe_raw(session, uid)
        await probe_client(session, uid)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
