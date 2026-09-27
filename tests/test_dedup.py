"""不依赖 AstrBot 运行时的自检脚本：去重 / 撤回 / 状态持久化。

直接运行：python tests/test_dedup.py
覆盖：状态历史持久化、推送台账去重、_process_items 的重复推送防护、
      检测到重复推送时的自动撤回。
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ---- 伪造 astrbot.api.logger，让模块可以在无 AstrBot 环境导入 ----
if "astrbot" not in sys.modules:
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")

    class _Logger:
        def _log(self, msg, *args, **kwargs):
            try:
                text = msg % args if args else msg
            except Exception:
                text = str(msg)
            print(f"[log] {text}")

        info = debug = warning = _log

        def exception(self, msg, *args, **kwargs):
            self._log(msg, *args, **kwargs)

    api.logger = _Logger()
    astrbot.api = api
    sys.modules["astrbot"] = astrbot
    sys.modules["astrbot.api"] = api

import astrbot_plugin_bili_subscription.subscription as S  # noqa: E402

FAILURES: list[str] = []


def check(condition: bool, label: str) -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}")
        FAILURES.append(label)


class Item:
    """模拟 UserDynamic / UserVideo / UserArticle。"""

    __slots__ = ("bvid", "dynamic_id", "article_id")

    def __init__(self, item_id: str) -> None:
        self.bvid = item_id
        self.dynamic_id = item_id
        self.article_id = item_id


async def test_state_history(tmp: Path) -> None:
    print("\n[1] 状态文件：最近推送历史")
    path = tmp / "state.json"
    store = S.SubscriptionStateStore(path)
    await store.load()
    # 按真实推送顺序（旧 -> 新）记录
    await store.set("1", "dynamic", "D1")
    for item_id in ("D2", "D3", "D4", "D5"):
        await store.remember("1", "dynamic", item_id)
    await store.save()

    raw = json.loads(path.read_text(encoding="utf-8"))
    check("|" in raw["1"]["dynamic"], "历史以列表形式落盘")
    check(raw["1"]["dynamic"].split("|")[0] == "D5", "最新一条在最前面")

    reloaded = S.SubscriptionStateStore(path)
    await reloaded.load()
    check(await reloaded.get("1", "dynamic") == "D5", "last 正确恢复")
    recent = await reloaded.recent("1", "dynamic")
    check(
        recent == ("D5", "D4", "D3", "D2", "D1"),
        f"历史正确恢复（{recent}）",
    )
    check(
        len(recent) <= S.SubscriptionStateStore.RECENT_LIMIT,
        "历史长度受容量限制",
    )

    # 兼容旧格式：纯字符串
    legacy = tmp / "legacy.json"
    legacy.write_text(
        json.dumps({"9": {"dynamic": "OLD1", "video_ts": "123"}}),
        encoding="utf-8",
    )
    store2 = S.SubscriptionStateStore(legacy)
    await store2.load()
    check(await store2.get("9", "dynamic") == "OLD1", "兼容旧格式 last")
    check(await store2.recent("9", "dynamic") == ("OLD1",), "旧格式进入历史")


async def test_ledger(tmp: Path) -> None:
    print("\n[2] 推送台账")
    path = tmp / "ledger.json"
    ledger = S.PushLedger(path)
    await ledger.load()
    await ledger.record(
        uid="1", kind="dynamic", item_id="D1",
        session="s1", message_id="1001",
    )
    # 同会话的后续消息（如合并转发）应追加，而不是覆盖
    await ledger.record(
        uid="1", kind="dynamic", item_id="D1",
        session="s1", message_id="1002",
    )
    await ledger.save()

    reloaded = S.PushLedger(path)
    await reloaded.load()
    check(
        await reloaded.delivered_sessions("1", "dynamic", "D1") == frozenset({"s1"}),
        "台账跨进程恢复送达记录",
    )
    check(
        await reloaded.message_ids("1", "dynamic", "D1") == ("1001", "1002"),
        "同一会话的多条消息 ID 全部保留（撤回时才能清干净）",
    )
    check(reloaded.message_count() == 2, "消息计数正确")
    check(
        await reloaded.pending_sessions("1", "dynamic", "D1", ("s1", "s2"))
        == ("s2",),
        "只补发未送达会话",
    )
    check(
        await reloaded.known_item_ids("1", "dynamic") == frozenset({"D1"}),
        "已知内容 ID 集合",
    )

    # 兼容旧格式：session -> 单个字符串
    legacy = tmp / "ledger_legacy.json"
    legacy.write_text(
        json.dumps({"1\x1fdynamic\x1fD9": {"s1": "777"}}),
        encoding="utf-8",
    )
    old = S.PushLedger(legacy)
    await old.load()
    check(
        await old.message_ids("1", "dynamic", "D9") == ("777",),
        "兼容旧的单字符串台账格式",
    )


def _make_pusher(tmp: Path, name: str, **kwargs):
    from astrbot_plugin_bili_subscription.pusher import SubscriptionPusher

    class FakeClient:
        def in_cooldown(self):
            return False

        async def download_image(self, *_a, **_k):
            return None

    store = S.SubscriptionStateStore(tmp / f"{name}_state.json")
    ledger = S.PushLedger(tmp / f"{name}_ledger.json")
    pusher = SubscriptionPusher(
        client=FakeClient(),
        store=store,
        ledger=ledger,
        send_callback=lambda *_: None,
        build_dynamic_chain=lambda *_: None,
        send_video_to_sessions=lambda *_: None,
        **kwargs,
    )
    return pusher, store, ledger


async def test_process_items_duplicate(tmp: Path) -> None:
    print("\n[3] _process_items：重复推送防护")
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        print("  SKIP  未安装 aiohttp，无法导入 pusher")
        return

    from astrbot_plugin_bili_subscription.pusher import SendOutcome

    # 动态 ID 在真实数据里是纯数字（如 1252235257501974532），
    # 这里也用小整数模拟，才能覆盖"水位线比较"分支。
    sent: list[tuple[str, str]] = []

    async def send_one(item, sessions):
        for session in sessions:
            sent.append((item.dynamic_id, session))
        return SendOutcome(delivered=set(sessions))

    pusher, store, _ledger = _make_pusher(tmp, "t3")
    await store.load()
    await pusher.ledger.load()

    sub = S.Subscription(uid="7", sessions=("s1",), types=frozenset({"dynamic"}))

    # 首次：只记录，不推送
    await pusher._process_items(sub, [Item("100"), Item("99")], "dynamic", send_one)
    check(sent == [], "首次检查不推送")
    check(await store.get("7", "dynamic") == "100", "首次记录最新 ID")

    # 第二次：窗口里出现 3 条新动态
    await pusher._process_items(
        sub,
        [Item("103"), Item("102"), Item("101"), Item("100")],
        "dynamic", send_one,
    )
    check(
        sent == [("101", "s1"), ("102", "s1"), ("103", "s1")],
        f"按从旧到新推送新条目（实际 {sent}）",
    )
    check(
        await store.recent("7", "dynamic")
        == ("103", "102", "101", "100", "99"),
        f"推送后历史顺序为 新→旧（实际 {await store.recent('7', 'dynamic')}）",
    )
    check(
        await store.get("7", "dynamic") == "103",
        "last 推进到本轮最新条目",
    )
    sent.clear()

    # 第三次：同一批内容再次出现在窗口 —— 必须一条都不发
    await pusher._process_items(
        sub,
        [Item("103"), Item("102"), Item("101"), Item("100")],
        "dynamic", send_one,
    )
    check(sent == [], f"同一批内容不会重复推送（实际 {sent}）")

    # 第四次：last 滑出窗口（模拟两次检查之间新增条目超过窗口）。
    # 水位线可用（纯数字 ID）时，明确比历史更新的条目应该被推送
    await pusher._process_items(
        sub,
        [Item("110"), Item("109"), Item("108"), Item("107")],
        "dynamic", send_one,
    )
    check(
        sent == [("107", "s1"), ("108", "s1"), ("109", "s1"), ("110", "s1")],
        f"last 滑出窗口时按水位线补推更新条目（实际 {sent}）",
    )
    check(await store.get("7", "dynamic") == "110", "水位线补推后 last 正确")
    sent.clear()

    # 第五次：窗口里全是已知条目 —— 水位线判断不能误推
    await pusher._process_items(
        sub, [Item("110"), Item("109"), Item("108")], "dynamic", send_one
    )
    check(sent == [], f"窗口内已知条目不会重推（实际 {sent}）")
    sent.clear()

    # 第六次：除 111 外全部已知 —— 只推 111
    await pusher._process_items(
        sub, [Item("111"), Item("110"), Item("109")], "dynamic", send_one
    )
    check(sent == [("111", "s1")], f"新条目正常推送（实际 {sent}）")
    sent.clear()

    # 第七次：ID 形态混杂（无法比较水位线）时必须保守跳过，绝不重推
    mixed = [Item("BV1xx411c7mD"), Item("1252235257501974532"),
             Item("1252235257501974531")]
    await store.set("7", "dynamic", "Zz9")
    await pusher._process_items(sub, mixed, "dynamic", send_one)
    check(sent == [], f"ID 形态混杂时保守跳过（实际 {sent}）")
    sent.clear()

    # 第八次：模拟插件重启（台账 + 状态从磁盘恢复）
    reborn, reborn_store, reborn_ledger = _make_pusher(tmp, "t3r")
    # 复用同一份文件：直接指过去
    reborn_store = S.SubscriptionStateStore(tmp / "t3_state.json")
    await reborn_store.load()
    reborn_ledger = S.PushLedger(tmp / "t3_ledger.json")
    await reborn_ledger.load()
    reborn._store = reborn_store
    reborn._ledger = reborn_ledger
    await reborn._process_items(
        sub, [Item("111"), Item("110"), Item("109")], "dynamic", send_one
    )
    check(sent == [], f"重启后同一条内容不会重推（实际 {sent}）")


async def test_duplicate_recall(tmp: Path) -> None:
    print("\n[4] 重复推送 -> 自动撤回")
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        print("  SKIP  未安装 aiohttp，无法导入 pusher")
        return

    from astrbot_plugin_bili_subscription.pusher import SendOutcome

    pusher, store, ledger = _make_pusher(tmp, "t4")
    await store.load()
    await ledger.load()

    # 台账里已经有一条 D2 发到 s1 的记录（消息 777）
    await ledger.record(
        uid="7", kind="dynamic", item_id="D2",
        session="s1", message_id="777",
    )

    recalled: list[tuple[str, tuple[str, ...]]] = []

    async def recall(session, message_ids):
        recalled.append((session, tuple(message_ids)))
        return len(message_ids)

    sent: list[str] = []

    async def send_one(item, sessions):
        sent.append(item.dynamic_id)
        return SendOutcome(delivered=set(sessions))

    pusher.set_recall_callback(recall)
    sub = S.Subscription(uid="7", sessions=("s1",), types=frozenset({"dynamic"}))
    await store.set("7", "dynamic", "D1")

    await pusher._process_items(sub, [Item("D2"), Item("D1")], "dynamic", send_one)
    check(sent == [], f"台账里已送达的条目不再发送（实际 {sent}）")
    check(recalled == [], "正常路径（未发送）不需要撤回")

    # 直接调用 _resend_guard：模拟"消息已经发出去了才发现重复"
    sent.clear()
    recalled.clear()
    handled = await pusher._resend_guard("7", "dynamic", "D2", ("s1",))
    check(handled is True, "_resend_guard 拦截重复发送")
    check(
        recalled == [("s1", ("777",))],
        f"按消息 ID 撤回重复消息（实际 {recalled}）",
    )
    check(
        await ledger.delivered_sessions("7", "dynamic", "D2") == frozenset({"s1"}),
        "撤回后仍保留已送达结论，避免下一轮再推",
    )


async def main() -> int:
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)
        await test_state_history(tmp)
        await test_ledger(tmp)
        await test_process_items_duplicate(tmp)
        await test_duplicate_recall(tmp)

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
