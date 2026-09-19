"""订阅推送调度：轮询、过滤、渲染、发送。

策略：
- 首次检查某账号时只记录"当前最新"，不推送。
- 之后每次发现新条目，从旧到新推送，状态推进到"最后一条成功的条目"。
- 单条目连续失败超过 max_retries 次会被跳过，避免坏条目阻塞后续。
- 多个订阅并发检查，一个慢账号不阻塞其他。
- 每轮 tick 前尝试刷新配置，用户改订阅后无需重启。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

from astrbot.api import logger

from .bili_api import (
    BiliSubscriptionClient,
    UserArticle,
    UserDynamic,
    UserVideo,
)
from .render import render_card
from .subscription import Subscription, SubscriptionStateStore


SendCallback = Callable[[str, Any], Awaitable[bool]]
BuildDynamicChain = Callable[[bytes | None, list[bytes]], Any]
SendVideoToSessions = Callable[[tuple[str, ...], UserVideo], Awaitable[bool]]
ConfigRefresher = Callable[[], None]
SendOne = Callable[[Any], Awaitable[bool]]

_CARD_IMAGE_LIMIT = 6      # 卡片最多贴 6 张
_FORWARD_IMAGE_LIMIT = 15  # 合并转发最多 15 张


def is_empty_dynamic(dynamic: UserDynamic) -> bool:
    """动态既无文字也无图片时视为空动态。"""
    if dynamic.is_original_video:
        return False
    if (dynamic.text or "").strip():
        return False
    if dynamic.image_urls:
        return False
    return True


class SubscriptionPusher:
    def __init__(
        self,
        *,
        client: BiliSubscriptionClient,
        store: SubscriptionStateStore,
        send_callback: SendCallback,
        build_dynamic_chain: BuildDynamicChain,
        send_video_to_sessions: SendVideoToSessions,
        max_items_per_push: int = 5,
        font_path: str | None = None,
        scan_interval_seconds: int = 60,
        enable_video_push: bool = True,
        skip_video_dynamic: bool = True,
        skip_empty_dynamic: bool = True,
        skip_forward_dynamic: bool = True,
        fetch_article_images: bool = True,
        max_retries: int = 3,
        config_refresher: ConfigRefresher | None = None,
    ) -> None:
        self._client = client
        self._store = store
        self._send = send_callback
        self._build_dynamic_chain = build_dynamic_chain
        self._send_video_to_sessions = send_video_to_sessions
        self._max_items = max(1, max_items_per_push)
        self._font_path = font_path
        self._scan_interval = max(10, scan_interval_seconds)
        self._enable_video_push = enable_video_push
        self._skip_video_dynamic = skip_video_dynamic
        self._skip_empty_dynamic = skip_empty_dynamic
        self._skip_forward_dynamic = skip_forward_dynamic
        self._fetch_article_images = fetch_article_images
        self._max_retries = max(1, max_retries)
        self._config_refresher = config_refresher

        self._subscriptions: list[Subscription] = []
        self._last_check: dict[str, float] = {}
        self._fail_counts: dict[tuple[str, str], int] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # 外部接口
    # ------------------------------------------------------------------
    def update_subscriptions(self, subs: list[Subscription]) -> None:
        self._subscriptions = list(subs)

    def update_options(
        self,
        *,
        enable_video_push: bool,
        skip_video_dynamic: bool,
        skip_empty_dynamic: bool,
        skip_forward_dynamic: bool,
        fetch_article_images: bool,
        max_items_per_push: int,
        scan_interval_seconds: int,
        font_path: str | None,
    ) -> None:
        """热重载除订阅列表之外的开关，无需重启插件。"""
        self._enable_video_push = enable_video_push
        self._skip_video_dynamic = skip_video_dynamic
        self._skip_empty_dynamic = skip_empty_dynamic
        self._skip_forward_dynamic = skip_forward_dynamic
        self._fetch_article_images = fetch_article_images
        self._max_items = max(1, max_items_per_push)
        self._scan_interval = max(10, scan_interval_seconds)
        self._font_path = font_path

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(
                self._run(), name="bili-subscription-pusher"
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def force_check(self) -> list[str]:
        report: list[str] = []
        for sub in self._subscriptions:
            report.append(f"== UID {sub.uid} ==")
            errors = await self._check(sub)
            report.extend(f"   {e}" for e in errors)
        await self._safe_save()
        report.append("已完成一次立即检查。首次运行只记录当前最新，不推送。")
        return report

    async def push_latest_dynamic_and_article(
        self, uid: str, session_id: str
    ) -> list[str]:
        """测试命令用，不受过滤开关影响。"""
        lines: list[str] = ["（测试推送不受过滤开关影响）"]

        try:
            dynamics = await self._client.fetch_dynamics(uid, limit=1)
        except Exception as exc:
            lines.append(f"动态抓取异常：{exc}")
            dynamics = []

        if dynamics:
            sub = Subscription(
                uid=uid,
                sessions=(session_id,),
                types=frozenset({"dynamic"}),
            )
            if await self._push_dynamic(sub, dynamics[0]):
                lines.append(f"已推送最新动态：{dynamics[0].dynamic_id}")
            else:
                lines.append("动态推送失败")
        else:
            lines.append("没有取到动态")

        try:
            articles = await self._client.fetch_articles(uid, limit=1)
        except Exception as exc:
            lines.append(f"专栏抓取异常：{exc}")
            articles = []

        if articles:
            article = articles[0]
            if self._fetch_article_images:
                content_images = await self._client.fetch_article_content_images(
                    article.article_id
                )
                if content_images:
                    article = self._with_content_images(article, content_images)
            sub = Subscription(
                uid=uid,
                sessions=(session_id,),
                types=frozenset({"article"}),
            )
            if await self._push_article(sub, article):
                lines.append(f"已推送最新专栏：{article.article_id}")
            else:
                lines.append("专栏推送失败")
        else:
            lines.append("没有取到专栏")

        return lines

    async def dry_run(self) -> list[str]:
        report: list[str] = []
        for sub in self._subscriptions:
            report.append(f"== UID {sub.uid} ==")
            report.append(f"   类型：{','.join(sorted(sub.types))}")
            if "video" in sub.types and self._enable_video_push:
                report.append(await self._dry_run_videos(sub))
            if "dynamic" in sub.types:
                report.append(await self._dry_run_dynamics(sub))
            if "article" in sub.types:
                report.append(await self._dry_run_articles(sub))
        return [line for line in report if line]

    # ------------------------------------------------------------------
    # 后台
    # ------------------------------------------------------------------
    async def _run(self) -> None:
        try:
            await asyncio.sleep(20)
        except asyncio.CancelledError:
            return
        while not self._stop.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("bili-subscription 调度循环异常")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._scan_interval)
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        if self._config_refresher is not None:
            try:
                self._config_refresher()
            except Exception:
                logger.exception("bili-subscription 刷新配置失败")

        now = time.monotonic()
        due: list[Subscription] = []
        for sub in self._subscriptions:
            last = self._last_check.get(sub.uid, 0.0)
            if now - last < sub.interval_minutes * 60:
                continue
            self._last_check[sub.uid] = now
            due.append(sub)
        if not due:
            return

        logger.debug("bili-subscription 本轮检查 %d 个订阅", len(due))
        await asyncio.gather(
            *(self._safe_check(sub) for sub in due), return_exceptions=True
        )
        await self._safe_save()

    async def _safe_check(self, sub: Subscription) -> None:
        try:
            await self._check(sub)
        except Exception:
            logger.exception("bili-subscription 检查 UID %s 失败", sub.uid)

    async def _safe_save(self) -> None:
        try:
            await self._store.save()
        except Exception:
            logger.exception("bili-subscription 保存状态失败")

    async def _check(self, sub: Subscription) -> list[str]:
        errors: list[str] = []

        if "video" in sub.types and self._enable_video_push:
            try:
                await self._check_videos(sub)
            except Exception as exc:
                logger.exception("UID %s 视频检查失败", sub.uid)
                errors.append(f"视频检查异常：{exc}")

        if "dynamic" in sub.types:
            try:
                await self._check_dynamics(sub)
            except Exception as exc:
                logger.exception("UID %s 动态检查失败", sub.uid)
                errors.append(f"动态检查异常：{exc}")

        if "article" in sub.types:
            try:
                await self._check_articles(sub)
            except Exception as exc:
                logger.exception("UID %s 专栏检查失败", sub.uid)
                errors.append(f"专栏检查异常：{exc}")

        return errors

    # ------------------------------------------------------------------
    # 三类内容
    # ------------------------------------------------------------------
    async def _check_videos(self, sub: Subscription) -> None:
        videos = await self._client.fetch_videos(sub.uid, limit=self._max_items)
        if not videos:
            return
        await self._process_items(
            sub, videos, "video",
            partial(self._send_video_to_sessions, sub.sessions),
        )

    async def _check_dynamics(self, sub: Subscription) -> None:
        dynamics = await self._client.fetch_dynamics(sub.uid, limit=self._max_items)
        if not dynamics:
            return
        await self._process_items(
            sub, dynamics, "dynamic",
            partial(self._send_dynamic_one, sub),
        )

    async def _check_articles(self, sub: Subscription) -> None:
        articles = await self._client.fetch_articles(sub.uid, limit=self._max_items)
        if not articles:
            return

        # 若开启，为每条待推送的专栏拉一次详情取正文图
        if self._fetch_article_images:
            articles = await self._enrich_articles_with_content(articles)

        await self._process_items(
            sub, articles, "article",
            partial(self._push_article, sub),
        )

    async def _enrich_articles_with_content(
        self, articles: list[UserArticle]
    ) -> list[UserArticle]:
        """为专栏补上正文图；某条失败不影响其他条。"""
        enriched: list[UserArticle] = []
        for article in articles:
            try:
                content = await self._client.fetch_article_content_images(
                    article.article_id
                )
            except Exception:
                content = ()
            enriched.append(
                self._with_content_images(article, content) if content else article
            )
        return enriched

    @staticmethod
    def _with_content_images(
        article: UserArticle, content_images: tuple[str, ...]
    ) -> UserArticle:
        """合并封面 + 正文图（去重，保留顺序）。"""
        seen: set[str] = set()
        merged: list[str] = []
        for url in (*article.covers, *content_images):
            if url and url not in seen:
                seen.add(url)
                merged.append(url)
        return UserArticle(
            article_id=article.article_id,
            title=article.title,
            summary=article.summary,
            covers=tuple(merged[:3]),
            created_at=article.created_at,
            content_images=tuple(merged),
        )

    # ------------------------------------------------------------------
    # 核心：统一处理流程
    # ------------------------------------------------------------------
    async def _process_items(
        self, sub: Subscription, items: list, kind: str, send_one: SendOne,
    ) -> None:
        newest_id = self._id_of(items[0], kind)
        last = await self._store.get(sub.uid, kind)

        if last is None:
            await self._store.set(sub.uid, kind, newest_id)
            logger.info(
                "UID %s %s 首次记录 %s，本次不推送", sub.uid, kind, newest_id
            )
            return

        new_items, found = self._collect_new(items, last, kind)

        if not found:
            fallback = items[0]
            fallback_id = self._id_of(fallback, kind)
            if self._too_many_failures(kind, fallback_id):
                logger.warning(
                    "UID %s %s 最新条目 %s 已达失败上限，跳过本轮",
                    sub.uid, kind, fallback_id,
                )
                return
            logger.warning(
                "UID %s %s 上次 %s 不在最近列表中，保守只推最新一条",
                sub.uid, kind, last,
            )
            new_items = [fallback]

        if not new_items:
            if newest_id != last:
                logger.warning(
                    "UID %s %s 新条目均被跳过，推进状态到 %s",
                    sub.uid, kind, newest_id,
                )
                await self._store.set(sub.uid, kind, newest_id)
            return

        logger.info("UID %s %s 发现新条目 %d 条", sub.uid, kind, len(new_items))

        last_success_id: str | None = None
        for item in reversed(new_items):
            item_id = self._id_of(item, kind)
            try:
                ok = await send_one(item)
            except Exception:
                logger.exception("推送 %s %s 异常", kind, item_id)
                ok = False

            if ok:
                self._note_success(kind, item_id)
                last_success_id = item_id
            else:
                self._note_failure(kind, item_id)
                logger.warning("推送 %s %s 失败，稍后重试", kind, item_id)
                break

        if last_success_id is not None:
            await self._store.set(sub.uid, kind, last_success_id)

    def _collect_new(
        self, items: list, last: str, kind: str
    ) -> tuple[list, bool]:
        new_items: list = []
        for item in items:
            item_id = self._id_of(item, kind)
            if item_id == last:
                return new_items, True
            if self._too_many_failures(kind, item_id):
                continue
            new_items.append(item)
        return new_items, False

    @staticmethod
    def _id_of(item: Any, kind: str) -> str:
        if kind == "video":
            return item.bvid
        if kind == "dynamic":
            return item.dynamic_id
        if kind == "article":
            return item.article_id
        return ""

    # ------------------------------------------------------------------
    # 失败计数
    # ------------------------------------------------------------------
    def _too_many_failures(self, kind: str, item_id: str) -> bool:
        return self._fail_counts.get((kind, item_id), 0) >= self._max_retries

    def _note_success(self, kind: str, item_id: str) -> None:
        self._fail_counts.pop((kind, item_id), None)

    def _note_failure(self, kind: str, item_id: str) -> None:
        key = (kind, item_id)
        count = self._fail_counts.get(key, 0) + 1
        self._fail_counts[key] = count
        if count == self._max_retries:
            logger.warning(
                "%s %s 连续失败 %d 次，将跳过后续重试",
                kind, item_id, count,
            )

    # ------------------------------------------------------------------
    # 干跑辅助
    # ------------------------------------------------------------------
    async def _dry_run_videos(self, sub: Subscription) -> str:
        try:
            videos = await self._client.fetch_videos(sub.uid, limit=self._max_items)
        except Exception as exc:
            return f"   视频抓取异常：{exc}"
        text = f"   视频候选：{len(videos)} 条"
        if videos:
            text += f"\n     最新：{videos[0].bvid} {videos[0].title[:40]}"
        return text

    async def _dry_run_dynamics(self, sub: Subscription) -> str:
        try:
            dynamics = await self._client.fetch_dynamics(
                sub.uid, limit=self._max_items
            )
        except Exception as exc:
            return f"   动态抓取异常：{exc}"
        lines = [f"   动态候选：{len(dynamics)} 条"]
        if dynamics:
            latest = dynamics[0]
            flags = []
            if latest.is_original_video:
                flags.append("视频动态")
            if latest.is_pure_forward:
                flags.append("纯转发")
            if is_empty_dynamic(latest):
                flags.append("空动态")
            flag_text = ("  " + "/".join(flags)) if flags else ""
            lines.append(
                f"     最新：{latest.dynamic_id} "
                f"kind={latest.kind or '?'}{flag_text} "
                f"图 {len(latest.image_urls)} 张"
            )
        return "\n".join(lines)

    async def _dry_run_articles(self, sub: Subscription) -> str:
        try:
            articles = await self._client.fetch_articles(
                sub.uid, limit=self._max_items
            )
        except Exception as exc:
            return f"   专栏抓取异常：{exc}"
        lines = [f"   专栏候选：{len(articles)} 条"]
        if articles:
            latest = articles[0]
            lines.append(
                f"     最新：{latest.article_id} 标题 {latest.title[:40]}"
            )
            if self._fetch_article_images:
                try:
                    content = await self._client.fetch_article_content_images(
                        latest.article_id
                    )
                except Exception as exc:
                    lines.append(f"     正文图拉取异常：{exc}")
                    content = ()
                lines.append(
                    f"     封面 {len(latest.covers)} 张，正文图 {len(content)} 张"
                )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 推送动作
    # ------------------------------------------------------------------
    async def _send_dynamic_one(
        self, sub: Subscription, dynamic: UserDynamic
    ) -> bool:
        if self._skip_video_dynamic and dynamic.is_original_video:
            logger.debug("跳过视频动态 %s", dynamic.dynamic_id)
            return True
        if self._skip_forward_dynamic and dynamic.is_pure_forward:
            logger.debug("跳过纯转发动态 %s", dynamic.dynamic_id)
            return True
        if self._skip_empty_dynamic and is_empty_dynamic(dynamic):
            logger.debug("跳过空动态 %s", dynamic.dynamic_id)
            return True
        return await self._push_dynamic(sub, dynamic)

    async def _push_dynamic(
        self, sub: Subscription, dynamic: UserDynamic
    ) -> bool:
        image_urls = dynamic.image_urls[:_CARD_IMAGE_LIMIT]
        image_bytes = await self._download_images(image_urls)
        card = await render_card(
            title="B 站动态更新",
            body=dynamic.text or "（无文字内容）",
            images=image_bytes,
            footer=f"https://t.bilibili.com/{dynamic.dynamic_id}",
            font_path=self._font_path,
        )
        return await self._broadcast(
            sub.sessions, card, image_bytes,
            content_id=dynamic.dynamic_id, kind="动态",
        )

    async def _push_article(
        self, sub: Subscription, article: UserArticle
    ) -> bool:
        # 有正文图就用合并后的列表，否则回退到封面
        all_urls = article.content_images or article.covers
        card_urls = all_urls[:_CARD_IMAGE_LIMIT]
        forward_urls = all_urls[:_FORWARD_IMAGE_LIMIT]

        card_images = await self._download_images(card_urls)
        forward_images = (
            card_images
            if len(forward_urls) == len(card_urls)
            else await self._download_images(forward_urls)
        )

        body = f"{article.title}\n\n{article.summary}"
        card = await render_card(
            title="B 站专栏更新",
            body=body,
            images=card_images,
            footer=f"https://www.bilibili.com/read/cv{article.article_id}",
            font_path=self._font_path,
        )
        return await self._broadcast(
            sub.sessions, card, forward_images,
            content_id=article.article_id, kind="专栏",
        )

    async def _broadcast(
        self,
        sessions: tuple[str, ...],
        card: bytes | None,
        image_bytes: list[bytes],
        *,
        content_id: str,
        kind: str,
    ) -> bool:
        if card is None and not image_bytes:
            logger.warning(
                "%s %s 无任何可发送内容（Pillow 不可用且无图片）",
                kind, content_id,
            )
            return False

        all_ok = True
        for session_id in sessions:
            try:
                chain = self._build_dynamic_chain(card, image_bytes)
                ok = await self._send(session_id, chain)
                if not ok:
                    all_ok = False
            except Exception:
                logger.exception(
                    "推送%s %s 到 %s 失败", kind, content_id, session_id
                )
                all_ok = False
        return all_ok

    async def _download_images(self, urls: tuple[str, ...]) -> list[bytes]:
        result: list[bytes] = []
        for url in urls[:6]:
            data = await self._client.download_image(url)
            if data:
                result.append(data)
        return result