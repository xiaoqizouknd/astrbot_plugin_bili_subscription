"""不依赖 AstrBot 运行时的 main.py 冒烟测试。

覆盖：
- main.py 能在最小 astrbot stub 下导入；
- 发送路径（OneBot call_action）能拿到消息 ID 并记录；
- 拆分链（卡片 + 合并转发）走各自的接口；
- _recall_messages 会调用 delete_msg。

直接运行：python tests/test_send_path.py
"""

from __future__ import annotations

import asyncio
import base64
import sys
import types
from enum import Enum
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FAILURES: list[str] = []


def check(condition: bool, label: str) -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}")
        FAILURES.append(label)


# ----------------------------------------------------------------------
# 最小 astrbot stub
# ----------------------------------------------------------------------
class MessageType(str, Enum):
    GROUP_MESSAGE = "GroupMessage"
    FRIEND_MESSAGE = "FriendMessage"


def _install_stubs() -> None:
    if "astrbot" in sys.modules:
        return

    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    core = types.ModuleType("astrbot.core")
    platform = types.ModuleType("astrbot.core.platform")
    msg_session = types.ModuleType("astrbot.core.platform.message_session")
    mtype = types.ModuleType("astrbot.core.platform.message_type")
    event_mod = types.ModuleType("astrbot.api.event")
    comp_mod = types.ModuleType("astrbot.api.message_components")
    star_mod = types.ModuleType("astrbot.api.star")
    result_mod = types.ModuleType("astrbot.core.message.message_event_result")
    mtype.MessageType = MessageType

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

    class _Filter:
        @staticmethod
        def command(*_a, **_k):
            def deco(func):
                return func
            return deco

    class AstrMessageEvent:
        pass

    event_mod.filter = _Filter()
    event_mod.AstrMessageEvent = AstrMessageEvent

    class _Component:
        type = "text"

        def toDict(self):
            return {"type": self.type, "data": {}}

    class Plain(_Component):
        type = "text"

        def __init__(self, text):
            self.text = text

        def toDict(self):
            return {"type": "text", "data": {"text": self.text}}

    class Image(_Component):
        type = "image"

        def __init__(self, file=None):
            self.file = file

        async def convert_to_base64(self):
            raw = str(self.file or "")
            if raw.startswith("base64://"):
                return raw[len("base64://"):]
            return base64.b64encode(raw.encode("utf-8")).decode("ascii")

        def toDict(self):
            return {"type": "image", "data": {"file": self.file}}

    class Video(_Component):
        type = "video"

        def __init__(self, file=None):
            self.file = file

        @classmethod
        def fromFileSystem(cls, path):
            return cls(file=path)

        def toDict(self):
            return {"type": "video", "data": {"file": self.file}}

    class Node(_Component):
        def __init__(self, content=None, name="", uin=""):
            self.content = content
            self.name = name
            self.uin = uin

    class Nodes(_Component):
        def __init__(self, nodes=None):
            self.nodes = nodes or []

        async def to_dict(self):
            return {"messages": []}

    for name, obj in (
        ("Image", Image), ("Plain", Plain), ("Video", Video),
        ("Node", Node), ("Nodes", Nodes),
    ):
        setattr(comp_mod, name, obj)

    class Star:
        def __init__(self, context=None, config=None):
            self.context = context
            self.config = config or {}

    class StarTools:
        @staticmethod
        def get_data_dir(_name):
            return Path("./_selftest_data")

    class Context:
        def __init__(self):
            self._insts: dict[str, object] = {}

        def get_config(self):
            return {}

        def get_platform_inst(self, platform_id):
            return self._insts.get(platform_id)

    star_mod.Star = Star
    star_mod.StarTools = StarTools
    star_mod.Context = Context

    class MessageChain:
        def __init__(self, chain=None):
            self.chain = chain or []

    result_mod.MessageChain = MessageChain

    class MessageSession:
        def __init__(self, platform_id, message_type, session_id):
            self.platform_name = platform_id
            self.platform_id = platform_id
            self.message_type = message_type
            self.session_id = session_id

        @staticmethod
        def from_str(session_str):
            platform_id, message_type, session_id = session_str.split(":", 2)
            return MessageSession(
                platform_id, MessageType(message_type), session_id
            )

    msg_session.MessageSesion = MessageSession
    msg_session.MessageSession = MessageSession

    # 注册进 sys.modules
    sys.modules.update(
        {
            "astrbot": astrbot,
            "astrbot.api": api,
            "astrbot.api.event": event_mod,
            "astrbot.api.message_components": comp_mod,
            "astrbot.api.star": star_mod,
            "astrbot.core": core,
            "astrbot.core.platform": platform,
            "astrbot.core.platform.message_session": msg_session,
            "astrbot.core.platform.message_type": mtype,
            "astrbot.core.message": types.ModuleType("astrbot.core.message"),
            "astrbot.core.message.message_event_result": result_mod,
        }
    )
    astrbot.api = api
    api.event = event_mod
    api.message_components = comp_mod
    api.star = star_mod
    core.platform = platform
    core.message = sys.modules["astrbot.core.message"]
    platform.message_session = msg_session
    platform.message_type = mtype


class FakeBot:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.next_id = 1000

    async def call_action(self, action, **params):
        self.calls.append((action, params))
        if action in ("send_group_msg", "send_private_msg"):
            self.next_id += 1
            return {"message_id": self.next_id}
        if action == "delete_msg":
            return {}
        return {}


class FakePlatform:
    def __init__(self, bot):
        self.bot = bot


async def main() -> int:
    _install_stubs()

    from astrbot.core.platform.message_session import MessageSesion

    import astrbot_plugin_bili_subscription.main as M
    from astrbot_plugin_bili_subscription.pusher import SendOutcome

    print("\n[1] main.py 导入与结构")
    check(hasattr(M, "BiliSubscriptionPlugin"), "插件类存在")
    check(hasattr(M, "_onebot_segments"), "_onebot_segments 存在")
    for method in (
        "_send_chain_raw", "_recall_messages", "_platform_bot",
        "_send_video_to_sessions", "_build_dynamic_chain", "cmd_recall",
    ):
        check(
            hasattr(M.BiliSubscriptionPlugin, method), f"方法 {method} 存在"
        )

    plugin = M.BiliSubscriptionPlugin.__new__(M.BiliSubscriptionPlugin)
    plugin._config = {}
    plugin._recall_enabled = True
    plugin._max_video_send_bytes = 0
    plugin._downloader = None
    plugin._client = None

    bot = FakeBot()
    plugin._context = None

    class _Ctx:
        def __init__(self):
            self._insts = {"default": FakePlatform(bot)}

        def get_config(self):
            return {}

        def get_platform_inst(self, platform_id):
            return self._insts.get(platform_id)

    plugin.context = _Ctx()

    print("\n[2] 消息链 -> OneBot 消息段")
    chain = M.MessageChain([M.Plain("hello"), M.Image(file="base64://abc")])
    segments = await M._onebot_segments(chain)
    check(
        segments == [
            {"type": "text", "data": {"text": "hello"}},
            {"type": "image", "data": {"file": "base64://abc"}},
        ],
        f"消息段转换正确（实际 {segments}）",
    )

    print("\n[3] 发送并捕获消息 ID")
    outcome = await plugin._send_chain_raw(
        "default:GroupMessage:1065310446", chain
    )
    check(
        outcome.delivered == {"default:GroupMessage:1065310446"},
        f"送达会话正确（实际 {outcome.delivered}）",
    )
    check(
        outcome.message_ids.get("default:GroupMessage:1065310446") == ["1001"],
        f"捕获消息 ID（实际 {outcome.message_ids}）",
    )
    action, params = bot.calls[-1]
    check(action == "send_group_msg", f"调用 send_group_msg（实际 {action}）")
    check(params.get("group_id") == 1065310446, "群号正确")

    print("\n[4] 撤回消息 ID")
    recalled = await plugin._recall_messages(
        "default:GroupMessage:1065310446", ("1001",)
    )
    check(recalled == 1, f"撤回成功计数（实际 {recalled}）")
    action, params = bot.calls[-1]
    check(action == "delete_msg", f"调用 delete_msg（实际 {action}）")
    check(params.get("message_id") == 1001, f"撤回参数为整数 ID（实际 {params}）")

    print("\n[5] 拆分链（卡片 + 合并转发）分段发送")
    bot.calls.clear()
    split = M._SplitChain(
        first=M.MessageChain([M.Image(file="base64://card")]),
        forward=M.MessageChain([M.Nodes([])]),
    )
    outcome = await plugin._send_chain_raw(
        "default:GroupMessage:1065310446", split
    )
    sends = [c for c in bot.calls if c[0] == "send_group_msg"]
    forwards = [c for c in bot.calls if c[0] == "send_group_forward_msg"]
    check(len(sends) == 1, f"卡片单独一条消息（实际 {len(sends)} 次 send）")
    check(len(forwards) == 1, f"合并转发单独一条消息（实际 {len(forwards)} 次）")
    check(
        outcome.delivered == {"default:GroupMessage:1065310446"},
        "拆分链部分成功即视为送达",
    )
    split_ids = outcome.message_ids.get("default:GroupMessage:1065310446") or []
    check(
        split_ids == ["1002"],
        f"卡片消息 ID 被记录（合并转发接口不返回 ID，实际 {split_ids}）",
    )

    print("\n[6] 无法解析会话时不应崩溃")
    outcome = await plugin._send_chain_raw("garbage-session", chain)
    check(outcome.delivered == set(), "非法会话返回空结果而非异常")

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
