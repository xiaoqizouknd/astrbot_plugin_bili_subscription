"""风控逻辑自测：熔断冷却必须真的生效。

不联网：用假 session 断言"冷却期内不再发请求"、"成功请求不会清空风控计数"。

用法：python tests/test_risk_control.py
"""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 伪造 logger，便于在无 AstrBot 环境下导入
if "astrbot" not in sys.modules:
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")

    class _Logger:
        def _log(self, msg, *args, **kwargs):
            pass

        info = debug = warning = _log

        def exception(self, msg, *args, **kwargs):
            self._log(msg, *args, **kwargs)

    api.logger = _Logger()
    astrbot.api = api
    sys.modules["astrbot"] = astrbot
    sys.modules["astrbot.api"] = api

import astrbot_plugin_bili_subscription.bili_api as B  # noqa: E402

FAILURES: list[str] = []


def check(condition: bool, label: str) -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}")
        FAILURES.append(label)


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.status = 200

    async def text(self) -> str:
        import json

        return json.dumps(self._payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def release(self):
        return None


class FakeSession:
    """记录每次请求，并按脚本返回 payload。"""

    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = list(payloads)
        self.calls: list[dict] = []
        self.cookie_jar = types.SimpleNamespace(update_cookies=lambda *a, **k: None)

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        payload = self.payloads.pop(0) if self.payloads else {"code": 0, "data": {}}
        return FakeResponse(payload)


def make_client(session: FakeSession) -> B.BiliSubscriptionClient:
    return B.BiliSubscriptionClient(session, min_request_gap=0.0)


async def test_cooldown_blocks_requests() -> None:
    print("\n[1] 熔断冷却期必须停止一切接口请求")
    # 连续 3 次风控 -> 进入冷却
    session = FakeSession([{"code": -352, "message": "risk"}] * 10)
    client = make_client(session)
    for _ in range(B._RISK_STREAK_THRESHOLD):
        client._slow_down()
        client._note_risk_error()
    check(client.in_cooldown(), "连续风控后进入冷却期")

    before = len(session.calls)
    try:
        await client._request("https://api.bilibili.com/x/test", {}, wbi=True)
        raised = False
    except B.BiliApiError as exc:
        raised = True
        code = exc.code
    check(raised, "冷却期内 _request 直接抛错")
    check(code == -352, "抛出的错误码标记为风控（不触发普通重试）")
    check(
        len(session.calls) == before,
        f"冷却期内没有发出任何 HTTP 请求（实际多出 {len(session.calls) - before} 次）",
    )

    # 冷却期内也不该刷新 bili_ticket
    refreshed = await client.refresh_bili_ticket()
    check(refreshed is False, "冷却期内不刷新 bili_ticket")
    check(
        len(session.calls) == before,
        "冷却期内没有发出 ticket 请求",
    )


async def test_success_does_not_clear_risk() -> None:
    print("\n[2] 个别请求成功不能清空风控计数（否则熔断永远不触发）")
    ok = {"code": 0, "data": {"list": {"vlist": []}}}
    session = FakeSession([ok] * 50)
    client = make_client(session)

    for _ in range(B._RISK_STREAK_THRESHOLD - 1):
        client._slow_down()
        client._note_risk_error()
    check(client._risk_streak == B._RISK_STREAK_THRESHOLD - 1, "风控计数已累积")

    # 一次成功请求混在中间
    await client._request_with_backoff(
        "https://api.bilibili.com/x/test", {}, wbi=False
    )
    check(
        client._risk_streak == B._RISK_STREAK_THRESHOLD - 1,
        f"成功请求后风控计数保持不变（实际 {client._risk_streak}）",
    )

    # 再来一次风控就该熔断
    client._slow_down()
    client._note_risk_error()
    check(client.in_cooldown(), "补足阈值后成功进入冷却期")


async def test_risk_error_not_retried_thrice() -> None:
    print("\n[3] 风控错误不再连打三次接口")
    risk = {"code": -352, "message": "risk"}
    session = FakeSession([risk] * 10)
    # 节流设为 0，避免测试变慢；风控探测延迟也调小
    original_delay = B._RISK_RETRY_DELAY_SECONDS
    B._RISK_RETRY_DELAY_SECONDS = 0.01
    try:
        client = make_client(session)
        try:
            await client._request_with_backoff(
                "https://api.bilibili.com/x/test", {}, wbi=False
            )
        except B.BiliApiError:
            pass
        check(
            len(session.calls) == 2,
            f"风控错误最多探测 2 次（实际 {len(session.calls)} 次）",
        )
    finally:
        B._RISK_RETRY_DELAY_SECONDS = original_delay


async def main() -> int:
    await test_cooldown_blocks_requests()
    await test_success_does_not_clear_risk()
    await test_risk_error_not_retried_thrice()

    print("\n" + "=" * 50)
    if FAILURES:
        print(f"{len(FAILURES)} 项失败：")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
