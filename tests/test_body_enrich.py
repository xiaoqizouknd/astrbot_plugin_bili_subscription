"""不依赖 AstrBot 运行时的自检脚本：正文补齐 + 多图排版。

直接运行：python tests/test_body_enrich.py

覆盖：
- _parse_dynamic 对"专栏动态"的正文补齐标记（may_need_body / article_id /
  image_layout）；
- _parse_opus_page 能从 opus 网页的 __INITIAL_STATE__ 里解析出完整段落与
  全部正文图片；
- strip_image_placeholders / merge_opus_body 的合并规则；
- fetch_dynamics 对 feed 偶发空返回的重试；
- render 的 layout 解析（grid / column / auto）。
"""

from __future__ import annotations

import asyncio
import io
import json
import sys
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
            return None

        info = debug = warning = _log

        def exception(self, msg, *args, **kwargs):
            return None

    api.logger = _Logger()
    astrbot.api = api
    sys.modules["astrbot"] = astrbot
    sys.modules["astrbot.api"] = api

import astrbot_plugin_bili_subscription.bili_api as B  # noqa: E402
import astrbot_plugin_bili_subscription.render as R  # noqa: E402
from astrbot_plugin_bili_subscription.bili_api import (  # noqa: E402
    OpusBody,
)
from astrbot_plugin_bili_subscription.pusher import (  # noqa: E402
    merge_opus_body,
)

FAILURES: list[str] = []


def check(condition: bool, label: str) -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}")
        FAILURES.append(label)


# ----------------------------------------------------------------------
# 样例数据
# ----------------------------------------------------------------------
# 结构与真实 opus 网页一致（只保留用到的部分）
OPUS_PAGE_HTML = """
<!DOCTYPE html><html><head><title>x</title></head><body>
<script>window.__INITIAL_STATE__ = %s;</script>
</body></html>
""" % json.dumps(
    {
        "detail": {
            "id_str": "1253057803877089317",
            "basic": {"rid_str": "53215280"},
            "modules": [
                {
                    "module_type": "MODULE_TYPE_TITLE",
                    "module_title": {"text": "公告：10月1日定期维护公告"},
                },
                {
                    "module_type": "MODULE_TYPE_AUTHOR",
                    "module_author": {
                        "name": "都市零协会",
                        "face": "//i0.hdslb.com/bfs/face/abc.jpg",
                        "pub_ts": 1790000000,
                    },
                },
                {
                    "module_type": "MODULE_TYPE_TOP",
                    "module_top": {
                        "display": {
                            "album": {"pics": [{"url": "//i0.hdslb.com/cover.png"}]}
                        }
                    },
                },
                {
                    "module_type": "MODULE_TYPE_CONTENT",
                    "module_content": {
                        "paragraphs": [
                            {
                                "para_type": 1,
                                "text": {
                                    "nodes": [
                                        {"word": {"words": "译：1 Hodron 2 族长"}}
                                    ]
                                },
                            },
                            {
                                "para_type": 1,
                                "text": {
                                    "nodes": [
                                        {"word": {"words": "预祝大家顺利。"}},
                                        {
                                            "type": "TEXT_NODE_TYPE_AT",
                                            "user": {"name": "@某人"},
                                        },
                                    ]
                                },
                            },
                            {
                                "para_type": 2,
                                "pic": {
                                    "pics": [
                                        {"url": "//i0.hdslb.com/body1.png"},
                                        {"url": "//i0.hdslb.com/body2.png"},
                                    ]
                                },
                            },
                            {
                                "para_type": 2,
                                "pic": {"pics": [{"url": "//i0.hdslb.com/body1.png"}]},
                            },
                        ]
                    },
                },
                {"module_type": "MODULE_TYPE_STAT", "module_stat": {}},
            ],
        }
    },
    ensure_ascii=False,
)


def article_feed_item() -> dict:
    """一条"专栏动态"的 feed 原始数据（正文被截断 + 图片被占位）。"""
    return {
        "id_str": "1253057803877089317",
        "type": "DYNAMIC_TYPE_ARTICLE",
        "basic": {"rid_str": "53215280", "comment_id_str": "53215280"},
        "modules": {
            "module_author": {"name": "都市零协会", "pub_ts": 1790000000},
            "module_dynamic": {
                "major": {
                    "opus": {
                        "title": "公告：10月1日定期维护公告",
                        "pics": [{"url": "http://i0.hdslb.com/cover.png"}],
                        "summary": {
                            "text": "译：1 Hodron\n[图片]\n[图片]",
                            "has_more": True,
                        },
                    }
                }
            },
        },
    }


def png_bytes(width: int, height: int) -> bytes:
    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (width, height), (30, 30, 30)).save(buf, format="PNG")
    return buf.getvalue()


# ----------------------------------------------------------------------
# [1] 解析：正文补齐标记
# ----------------------------------------------------------------------
def test_parse_dynamic_flags() -> None:
    print("\n[1] _parse_dynamic：正文补齐标记")
    parsed = B._parse_dynamic(article_feed_item())
    check(parsed is not None, "解析成功")
    assert parsed is not None
    check(parsed.may_need_body, "含 [图片] 占位符 → may_need_body")
    check(parsed.article_id == "53215280", f"取到 cv 号（{parsed.article_id}）")
    check(parsed.image_layout == "column", "专栏动态 → 正文长图单列")
    check("[图片]" in parsed.text, "未补齐前正文保留占位符提示")

    item = article_feed_item()
    item["modules"]["module_dynamic"]["major"]["opus"]["summary"]["text"] = "纯文字"
    parsed_text_only = B._parse_dynamic(item)
    assert parsed_text_only is not None
    check(
        parsed_text_only.may_need_body,
        "has_more=True（正文被截断）同样触发补齐",
    )

    draw = {
        "id_str": "1",
        "type": "DYNAMIC_TYPE_DRAW",
        "basic": {"rid_str": "410435587"},
        "modules": {
            "module_author": {"name": "UP", "pub_ts": 1},
            "module_dynamic": {
                "major": {
                    "draw": {
                        "items": [
                            {"src": "http://i0.hdslb.com/a.png"},
                            {"src": "http://i0.hdslb.com/b.png"},
                        ]
                    }
                }
            },
        },
    }
    parsed_draw = B._parse_dynamic(draw)
    assert parsed_draw is not None
    check(parsed_draw.image_layout == "grid", "图文动态 → 相册网格")
    check(len(parsed_draw.image_urls) == 2, "相册图全部保留")


# ----------------------------------------------------------------------
# [2] opus 网页解析
# ----------------------------------------------------------------------
def test_parse_opus_page() -> None:
    print("\n[2] _parse_opus_page：完整正文 + 全部正文图")
    body = B._parse_opus_page(OPUS_PAGE_HTML)
    check(body is not None, "解析成功")
    assert body is not None
    check(
        body.text.splitlines()[0] == "译：1 Hodron 2 族长",
        f"段落文本按顺序（{body.text.splitlines()[0]!r}）",
    )
    check("预祝大家顺利。@某人" in body.text, "文本节点拼接（含 @ 节点）")
    check(
        body.image_urls
        == (
            "https://i0.hdslb.com/body1.png",
            "https://i0.hdslb.com/body2.png",
        ),
        f"正文图去重且保序（{body.image_urls}）",
    )
    check(
        body.cover_urls == ("https://i0.hdslb.com/cover.png",),
        "顶部大图单独给出",
    )
    check(body.title == "公告：10月1日定期维护公告", "标题")
    check(body.author_face.startswith("https://"), "头像补全 https")
    check(body.created_at == 1790000000, "发布时间")

    check(B._parse_opus_page("") is None, "空 HTML → None")
    check(B._parse_opus_page("<html>没有状态</html>") is None, "无状态 → None")
    check(
        B._parse_opus_page(
            'window.__INITIAL_STATE__ = {"detail": {"modules": []}};'
        )
        is None,
        "空 modules → None",
    )


# ----------------------------------------------------------------------
# [3] 合并与占位符清理
# ----------------------------------------------------------------------
def test_merge_and_placeholder() -> None:
    print("\n[3] merge_opus_body / strip_image_placeholders")
    dynamic = B._parse_dynamic(article_feed_item())
    assert dynamic is not None
    body = B._parse_opus_page(OPUS_PAGE_HTML)
    assert body is not None

    merged = merge_opus_body(dynamic, body)
    check(
        merged.text.startswith("公告：10月1日定期维护公告\n\n译：1 Hodron"),
        f"正文 = 标题 + 完整段落（{merged.text[:24]!r}）",
    )
    check("[图片]" not in merged.text, "完整正文里没有占位符")
    check(
        merged.image_urls
        == (
            "http://i0.hdslb.com/cover.png",
            "https://i0.hdslb.com/body1.png",
            "https://i0.hdslb.com/body2.png",
        ),
        f"封面 + 正文图按顺序合并（{merged.image_urls}）",
    )
    check(not merged.may_need_body, "补齐后清除标记")

    # 网页解析失败（body.text 为空）时不能把正文弄丢
    fallback = merge_opus_body(
        dynamic, OpusBody(text="", cover_urls=("https://i0.hdslb.com/x.png",))
    )
    check(fallback.text == dynamic.text, "正文为空时保留原文")
    check(
        fallback.image_urls
        == ("http://i0.hdslb.com/cover.png", "https://i0.hdslb.com/x.png"),
        "图片仍然合并",
    )

    cleared = B.strip_image_placeholders("第一行\n[图片]\n第二行 [图片] 尾巴\n\n\n")
    check(
        "[图片]" not in cleared and "第一行" in cleared and "第二行" in cleared,
        f"占位符清理（{cleared!r}）",
    )
    check(
        B.strip_image_placeholders("没有占位符") == "没有占位符",
        "无占位符时原样返回",
    )


# ----------------------------------------------------------------------
# [4] feed 空返回重试
# ----------------------------------------------------------------------
class _FakeClient(B.BiliSubscriptionClient):
    """绕过 __init__，只测 _fetch_dynamic_items 的重试逻辑。"""

    def __init__(self, responses: list[list]) -> None:  # noqa: D107
        self._active_feeds = {}
        self._responses = list(responses)
        self.calls = 0

    async def _request_with_backoff(self, url, params, *, wbi, **kwargs):  # noqa: ANN001
        self.calls += 1
        items = self._responses.pop(0) if self._responses else []
        return {"items": items}


async def test_feed_empty_retry() -> None:
    print("\n[4] fetch_dynamics：feed 空返回重试")
    original_delay = B._EMPTY_FEED_RETRY_DELAY
    B._EMPTY_FEED_RETRY_DELAY = 0.0
    try:
        # 首次见到的 UID：空 → 空，2 次后放弃
        empty = _FakeClient([[], []])
        await empty._fetch_dynamic_items("1")
        check(
            empty.calls == B._EMPTY_FEED_ATTEMPTS_FIRST,
            f"首次UID空返回重试到 {B._EMPTY_FEED_ATTEMPTS_FIRST} 次",
        )

        # 已知有动态的 UID：空 → 空 → 有数据，第 3 次成功
        recovered = _FakeClient([[], [], [{"id_str": "9"}]])
        recovered._active_feeds["2"] = None
        items = await recovered._fetch_dynamic_items("2")
        check(
            len(items) == 1 and recovered.calls == 3,
            "已知有动态的 UID 会在后续重试里拿到数据",
        )

        # 第一次就有数据：不重试
        direct = _FakeClient([[{"id_str": "1"}]])
        await direct._fetch_dynamic_items("3")
        check(direct.calls == 1, "有数据时不额外请求")
        check("3" in direct._active_feeds, "记住该 UID 有动态")
    finally:
        B._EMPTY_FEED_RETRY_DELAY = original_delay


# ----------------------------------------------------------------------
# [5] 多图排版
# ----------------------------------------------------------------------
def test_render_layout() -> None:
    print("\n[5] render：多图排版")
    if not R.pil_available():
        print("  SKIP  未安装 Pillow")
        return

    from PIL import Image

    grid4 = [Image.new("RGB", (800, 800)) for _ in range(4)]
    column3 = [Image.new("RGB", (1950, 2100)) for _ in range(3)]
    auto_land = [Image.new("RGB", (1200, 600)) for _ in range(2)]

    check(R._resolve_columns("grid", grid4) == 2, "grid：4 张 → 两列")
    check(R._resolve_columns("grid", grid4[:3]) == 3, "grid：3 张 → 三列")
    check(R._resolve_columns("grid", grid4[:1]) == 1, "grid：1 张 → 整宽")
    check(R._resolve_columns("column", grid4) == 1, "column：正文长图整宽单列")
    check(
        R._resolve_columns("auto", column3) == 1,
        "auto：多张竖长图 → 单列（不再纵向拼成一张超长图）",
    )
    check(R._resolve_columns("auto", auto_land) == 2, "auto：横图 → 网格")

    # 4 张等尺寸图走网格：应产出一张画布，而不是 4 张堆叠
    images, height = R._prepare_images(
        [png_bytes(800, 800) for _ in range(4)], 812, layout="grid"
    )
    check(len(images) == 1, "网格排版合成单张画布")
    check(images[0].width <= 812 and height > 0, "画布宽度不超过卡片宽度")

    # 3 张同宽长图（旧逻辑会误判成"切片"竖拼）走单列：保留 3 张独立图
    images, _ = R._prepare_images(
        [png_bytes(1950, 2100) for _ in range(3)], 812, layout="column"
    )
    check(len(images) == 3, "单列排版保留独立图片（不误拼）")
    check(images[0].width >= 700, f"长图仍有可读宽度（{images[0].width}px）")


# ----------------------------------------------------------------------
# [6] 推送路径：补齐 + 排版串起来
# ----------------------------------------------------------------------
class _StubClient:
    """只实现推送路径用到的方法。"""

    def __init__(self, body: OpusBody) -> None:
        self._body = body
        self.opus_calls = 0
        self.downloaded: list[str] = []
        self.article_calls = 0

    async def enrich_dynamic(self, dynamic):  # noqa: ANN001, ANN201
        return dynamic

    async def fetch_opus_body(self, dynamic_id: str):  # noqa: ANN201
        self.opus_calls += 1
        return self._body

    async def fetch_article_body(self, article_id: str, *, limit: int = 6):  # noqa: ANN201
        from astrbot_plugin_bili_subscription.bili_api import ArticleBody

        self.article_calls += 1
        return ArticleBody(
            text="专栏正文第一段\n\n专栏正文第二段",
            image_urls=("https://i0.hdslb.com/art1.png",),
        )

    async def download_image(self, url: str, *, max_bytes: int = 0):  # noqa: ANN201
        self.downloaded.append(url)
        return png_bytes(1200, 900)

    def in_cooldown(self) -> bool:
        return False


async def test_push_path(tmp: Path) -> None:
    print("\n[6] 推送路径：正文补齐 + 排版参数")
    import astrbot_plugin_bili_subscription.pusher as P  # noqa: PLC0415
    from astrbot_plugin_bili_subscription.bili_api import (  # noqa: PLC0415
        UserArticle,
    )
    from astrbot_plugin_bili_subscription.subscription import (  # noqa: PLC0415
        PushLedger,
        Subscription,
        SubscriptionStateStore,
    )

    body = B._parse_opus_page(OPUS_PAGE_HTML)
    assert body is not None
    client = _StubClient(body)
    store = SubscriptionStateStore(tmp / "state.json")
    ledger = PushLedger(tmp / "ledger.json")

    recorded: dict = {}

    async def fake_dynamic_card(**kwargs):  # noqa: ANN003, ANN201
        recorded["dynamic"] = kwargs
        return b"card-bytes"

    async def fake_card(**kwargs):  # noqa: ANN003, ANN201
        recorded["article"] = kwargs
        return b"card-bytes"

    sent: list[tuple[str, object]] = []

    async def send_callback(session_id, chain):  # noqa: ANN001, ANN202
        from astrbot_plugin_bili_subscription.pusher import SendOutcome

        sent.append((session_id, chain))
        return SendOutcome(delivered={session_id})

    def build_chain(card, images, prefix):  # noqa: ANN001, ANN202
        return {"card": card, "images": len(images), "prefix": prefix}

    pusher = P.SubscriptionPusher(
        client=client,
        store=store,
        ledger=ledger,
        send_callback=send_callback,
        build_dynamic_chain=build_chain,
        send_video_to_sessions=None,  # 本用例不涉及视频
    )

    original_dynamic_render = P.render_dynamic_card
    original_render = P.render_card
    P.render_dynamic_card = fake_dynamic_card
    P.render_card = fake_card
    try:
        dynamic = B._parse_dynamic(article_feed_item())
        assert dynamic is not None
        sub = Subscription(
            uid="3632319835409017", sessions=("s1",), types=frozenset({"dynamic"})
        )
        outcome = await pusher._push_dynamic(sub, dynamic, ("s1",))
        check(outcome.delivered == {"s1"}, "动态推送成功")
        check(client.opus_calls == 1, "含 [图片] 的动态触发一次正文补齐")
        kwargs = recorded.get("dynamic") or {}
        check(kwargs.get("layout") == "column", "专栏动态按单列排版")
        check("[图片]" not in (kwargs.get("body") or ""), "卡片正文没有占位符")
        check(
            "预祝大家顺利。@某人" in (kwargs.get("body") or ""),
            "卡片正文用网页里的完整段落",
        )
        check(len(kwargs.get("images") or []) == 3,
              f"封面 + 2 张正文图都进卡片（{len(kwargs.get('images') or [])} 张）")
        check(
            len(sent) == 1 and sent[0][1]["images"] == 3,
            "消息链只构建一次并带全部图片",
        )

        # 专栏：正文用全文而不是列表接口的摘要
        article = UserArticle(
            article_id="53215280",
            title="公告：10月1日定期维护公告",
            summary="这是列表接口的摘要",
            covers=(),
            created_at=0,
        )
        sub_article = Subscription(
            uid="3632319835409017", sessions=("s1",), types=frozenset({"article"})
        )
        outcome = await pusher._push_article(sub_article, article, ("s1",))
        check(outcome.delivered == {"s1"}, "专栏推送成功")
        article_kwargs = recorded.get("article") or {}
        article_body = article_kwargs.get("body") or ""
        check("专栏正文第一段" in article_body, "专栏正文使用抓到的全文")
        check("这是列表接口的摘要" not in article_body, "不再只用列表摘要")
        check(article_kwargs.get("layout") == "column", "专栏整宽单列")
        check(
            client.downloaded[-1] == "https://i0.hdslb.com/art1.png",
            "专栏正文插图被下载",
        )
    finally:
        P.render_dynamic_card = original_dynamic_render
        P.render_card = original_render


async def main() -> int:
    import tempfile

    test_parse_dynamic_flags()
    test_parse_opus_page()
    test_merge_and_placeholder()
    await test_feed_empty_retry()
    test_render_layout()
    with tempfile.TemporaryDirectory() as raw_tmp:
        await test_push_path(Path(raw_tmp))

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
