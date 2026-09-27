"""订阅推送调度：轮询、过滤、渲染、发送。

策略：
- 首次检查某账号时只记录"当前最新"，不推送。
- 之后每次发现新条目，从旧到新推送，状态推进到"最后一条成功的条目"。
- **去重的唯一真相是推送台账（PushLedger）**：按「内容 ID + 会话」记账，
  与抓取窗口大小、进程重启都无关。同一条内容对同一个会话只会发出一次。
- 为了在"两次检查之间新增条目多于窗口"时也能正确判断新旧，
  状态文件会额外记住最近若干条已知 ID（而不只是最后一条）。
- 每个会话单独记录送达情况：部分会话失败时下一轮只补发失败的会话。
  同一会话对同一条目最多收到一次；一旦检测到重复，会撤回重复消息。
- 单条目连续失败超过 max_retries 次会被跳过，避免坏条目阻塞后续；
  整轮检查失败（如网络故障）2 分钟后快速重试，而不是等满整个间隔。
- 每个 UID 一把互斥锁：手动"订阅检查"与后台轮询重叠时不会重复推送。
- 多个订阅限并发检查（默认 2），防止大量请求同时打向 B 站。
- 每轮 tick 前尝试刷新配置，用户改订阅后无需重启。
- 每小时轻量检测一次 B 站登录态；每天刷新一次 bili_ticket 维持 Cookie 指纹。
- 支持静音时段：时段内暂停一切网络请求，结束后自动补推。
- 风控熔断：客户端连续遭遇风控进入冷却期时，后台轮询自动暂停，
  冷却结束自动恢复，避免被封期间继续请求加重风控。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from astrbot.api import logger

from .bili_api import (
    BiliSubscriptionClient,
    UserArticle,
    UserDynamic,
    UserVideo,
)
from .render import render_card, render_dynamic_card
from .subscription import (
    PushLedger,
    Subscription,
    SubscriptionStateStore,
)


@dataclass(slots=True)
class SendOutcome:
    """一次发送的结果：哪些会话送达了，各自的消息 ID 是什么。

    message_ids 用于撤回（同一条内容被重复发出时按 ID 撤回）。
    同一条内容可能由多条消息组成（卡片 + 合并转发），因此每个会话
    对应一个 ID 列表；平台拿不到消息 ID 时列表为空。
    """

    delivered: set[str] = field(default_factory=set)
    message_ids: dict[str, list[str]] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.delivered)

    @classmethod
    def empty(cls) -> "SendOutcome":
        return cls()

    @classmethod
    def union(cls, *outcomes: "SendOutcome") -> "SendOutcome":
        merged = cls()
        for outcome in outcomes:
            if outcome is None:
                continue
            merged.delivered.update(outcome.delivered)
            for session, ids in outcome.message_ids.items():
                bucket = merged.message_ids.setdefault(session, [])
                for message_id in ids:
                    if message_id not in bucket:
                        bucket.append(message_id)
        return merged


SendCallback = Callable[[str, Any], Awaitable[SendOutcome]]
BuildDynamicChain = Callable[[bytes | None, list[bytes], str | None], Any]
SendVideoToSessions = Callable[[tuple[str, ...], UserVideo], Awaitable[SendOutcome]]
ConfigRefresher = Callable[[], None]

# 失败计数字典的容量上限；超过时清理最早的一半
_FAIL_COUNT_MAX_KEYS = 500
# 整轮检查失败后的快速重试间隔（秒）
_FAIL_RETRY_SECONDS = 120
# bili_ticket 刷新间隔（秒）：约 3 天有效，每天刷一次
_TICKET_REFRESH_SECONDS = 86400


def is_empty_dynamic(dynamic: UserDynamic) -> bool:
    """动态既无文字也无图片时视为空动态。"""
    if dynamic.is_original_video:
        return False
    if (dynamic.text or "").strip():
        return False
    if dynamic.image_urls:
        return False
    return True


_GIF_MAGIC = (b"GIF87a", b"GIF89a")


def is_gif(data: bytes) -> bool:
    """按文件头识别 GIF（不看 URL 后缀，防止被查询参数干扰）。

    main.py 的消息构造也用它，把动图放进合并转发时保留原图动画。
    """
    return len(data) >= 6 and data[:6] in _GIF_MAGIC


class SubscriptionPusher:
    def __init__(
        self,
        *,
        client: BiliSubscriptionClient,
        store: SubscriptionStateStore,
        ledger: PushLedger | None = None,
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
        retry_failed_push: bool = False,
        max_retries: int = 3,
        config_refresher: ConfigRefresher | None = None,
        max_concurrent_checks: int = 2,
        quiet_enabled: bool = False,
        quiet_start_minutes: int | None = None,
        quiet_end_minutes: int | None = None,
    ) -> None:
        self._client = client
        self._store = store
        # 台账缺失时退化为内存台账，保证功能可用（重启后记忆丢失）
        self._ledger = ledger if ledger is not None else PushLedger("")
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
        self._retry_failed_push = retry_failed_push
        self._max_retries = max(1, max_retries)
        self._config_refresher = config_refresher

        # 静音时段（分钟数，从 0 点起算）
        self._quiet_enabled = quiet_enabled
        self._quiet_start = quiet_start_minutes
        self._quiet_end = quiet_end_minutes
        self._quiet_logged = False

        # 风控冷却期日志去重标记
        self._cooldown_logged = False

        # 限并发：防止同一时刻大量请求打向 B 站
        self._check_semaphore = asyncio.Semaphore(max(1, max_concurrent_checks))

        # 每个 UID 一把锁：手动"订阅检查"与后台轮询重叠时不会重复推送
        self._uid_locks: dict[str, asyncio.Lock] = {}

        # 每小时检测一次登录态；每天刷新一次 bili_ticket
        self._last_login_check = 0.0
        self._last_ticket_refresh = 0.0

        self._subscriptions: list[Subscription] = []
        self._last_check: dict[str, float] = {}
        self._fail_counts: dict[tuple[str, str], int] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        # 撤回回调：由 main.py 注入（按会话 + 消息 ID 撤回）
        self._recall_callback: (
            Callable[[str, tuple[str, ...]], Awaitable[int]] | None
        ) = None

    @property
    def ledger(self) -> PushLedger:
        return self._ledger

    def set_recall_callback(
        self, callback: Callable[[str, tuple[str, ...]], Awaitable[int]]
    ) -> None:
        self._recall_callback = callback

    def _uid_lock(self, uid: str) -> asyncio.Lock:
        lock = self._uid_locks.get(uid)
        if lock is None:
            lock = asyncio.Lock()
            self._uid_locks[uid] = lock
        return lock

    def _in_quiet_time(self) -> bool:
        """当前是否处于静音时段（支持跨零点，如 23:00-07:00）。"""
        if not self._quiet_enabled or self._quiet_start is None or self._quiet_end is None:
            return False
        start, end = self._quiet_start, self._quiet_end
        if start == end:
            return False
        now = time.localtime()
        now_minutes = now.tm_hour * 60 + now.tm_min
        if start < end:
            return start <= now_minutes < end
        return now_minutes >= start or now_minutes < end

    # ------------------------------------------------------------------
    # 外部接口
    # ------------------------------------------------------------------
    def update_subscriptions(self, subs: list[Subscription]) -> None:
        self._subscriptions = list(subs)
        # 清理已删除 UID 的检查时间戳，避免字典无限增长
        valid_uids = {s.uid for s in subs}
        self._last_check = {
            uid: ts for uid, ts in self._last_check.items() if uid in valid_uids
        }

    def update_options(
        self,
        *,
        enable_video_push: bool,
        skip_video_dynamic: bool,
        skip_empty_dynamic: bool,
        skip_forward_dynamic: bool = True,
        retry_failed_push: bool = False,
        max_items_per_push: int,
        scan_interval_seconds: int,
        font_path: str | None,
        quiet_enabled: bool = False,
        quiet_start_minutes: int | None = None,
        quiet_end_minutes: int | None = None,
    ) -> None:
        """热重载除订阅列表之外的开关，无需重启插件。"""
        self._enable_video_push = enable_video_push
        self._skip_video_dynamic = skip_video_dynamic
        self._skip_empty_dynamic = skip_empty_dynamic
        self._skip_forward_dynamic = skip_forward_dynamic
        self._retry_failed_push = retry_failed_push
        self._max_items = max(1, max_items_per_push)
        self._scan_interval = max(10, scan_interval_seconds)
        self._font_path = font_path
        self._quiet_enabled = quiet_enabled
        self._quiet_start = quiet_start_minutes
        self._quiet_end = quiet_end_minutes

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
        """立即执行一次完整检查。

        与后台 tick 不同，这里对每个订阅强制检查所有已配置类型，
        即使 ``enable_video_push=False`` 也会拉一次视频接口，
        以便"订阅检查"能反映真实抓取情况。
        """
        report: list[str] = []
        for sub in self._subscriptions:
            report.append(f"== UID {sub.uid} ==")
            async with self._uid_lock(sub.uid):
                errors = await self._check(sub, force_all=True)
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
            item = dynamics[0]
            if await self._resend_guard(uid, "dynamic", item.dynamic_id, (session_id,)):
                lines.append(f"最新动态 {item.dynamic_id} 此前已推送过，已跳过")
            else:
                sub = Subscription(
                    uid=uid,
                    sessions=(session_id,),
                    types=frozenset({"dynamic"}),
                )
                outcome = await self._push_dynamic(sub, item, sub.sessions)
                if outcome:
                    lines.append(f"已推送最新动态：{item.dynamic_id}")
                    # 测试推送也要记账 + 推进状态，
                    # 避免后台轮询把同一条再推一遍
                    await self._record_outcome(
                        uid, "dynamic", item.dynamic_id, outcome
                    )
                    await self._store.set(uid, "dynamic", item.dynamic_id)
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
            item = articles[0]
            if await self._resend_guard(uid, "article", item.article_id, (session_id,)):
                lines.append(f"最新专栏 {item.article_id} 此前已推送过，已跳过")
            else:
                sub = Subscription(
                    uid=uid,
                    sessions=(session_id,),
                    types=frozenset({"article"}),
                )
                outcome = await self._push_article(sub, item, sub.sessions)
                if outcome:
                    lines.append(f"已推送最新专栏：{item.article_id}")
                    await self._record_outcome(
                        uid, "article", item.article_id, outcome
                    )
                    await self._store.set(uid, "article", item.article_id)
                else:
                    lines.append("专栏推送失败")
        else:
            lines.append("没有取到专栏")

        await self._safe_ledger_save()
        return lines

    async def push_latest_video(self, uid: str, session_id: str) -> list[str]:
        """测试命令用：推送最新一条视频（不受过滤开关影响）。

        推送成功后记账并推进状态，避免后台轮询把同一条视频再推一遍。
        """
        try:
            videos = await self._client.fetch_videos(uid, limit=1)
        except Exception as exc:
            return [f"视频抓取异常：{exc}"]
        if not videos:
            return ["没有取到视频"]

        item = videos[0]
        if await self._resend_guard(uid, "video", item.bvid, (session_id,)):
            return [f"视频 {item.bvid} 此前已推送过，已跳过（避免重复）"]

        outcome = await self._send_video_to_sessions((session_id,), item)
        if outcome:
            await self._record_outcome(uid, "video", item.bvid, outcome)
            await self._store.set(uid, "video", item.bvid)
            await self._safe_ledger_save()
            return [f"已推送最新视频：{item.bvid}"]
        return ["视频推送失败"]

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
            # 启动后稍等片刻再进入轮询，让 AstrBot 各适配器完成加载
            await asyncio.sleep(10)
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
        # 每轮 tick 都尝试刷新配置，用户在 WebUI 改订阅后无需重启
        if self._config_refresher is not None:
            try:
                self._config_refresher()
            except Exception:
                logger.exception("bili-subscription 刷新配置失败")

        now = time.monotonic()

        # 静音时段：暂停一切网络请求与推送，结束后下一轮自动补推
        if self._in_quiet_time():
            if not self._quiet_logged:
                self._quiet_logged = True
                logger.info("bili-subscription 进入静音时段，暂停检查")
            return
        if self._quiet_logged:
            self._quiet_logged = False
            logger.info("bili-subscription 静音时段结束，恢复检查")

        # 风控熔断：客户端进入冷却期时暂停一切后台 B 站请求，
        # 冷却结束自动恢复，避免被封期间继续硬刚加重风控
        if self._client.in_cooldown():
            if not self._cooldown_logged:
                self._cooldown_logged = True
                logger.warning(
                    "bili-subscription 风控冷却中（剩余约 %d 秒），"
                    "暂停后台检查，冷却结束自动恢复",
                    self._client.cooldown_remaining_seconds(),
                )
            return
        if self._cooldown_logged:
            self._cooldown_logged = False
            logger.info("bili-subscription 风控冷却结束，恢复后台检查")

        # 每天刷新一次 bili_ticket，维持 Cookie 指纹新鲜度
        if now - self._last_ticket_refresh >= _TICKET_REFRESH_SECONDS:
            self._last_ticket_refresh = now
            try:
                await self._client.refresh_bili_ticket()
            except Exception:
                logger.exception("bili-subscription 刷新 bili_ticket 失败")

        # 每小时轻量检测一次登录态
        if now - self._last_login_check > 3600:
            self._last_login_check = now
            try:
                await self._client.check_login()
            except Exception:
                logger.exception("bili-subscription 登录态检测异常")

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

    async def _safe_check(self, sub: Subscription) -> None:
        async with self._check_semaphore:
            async with self._uid_lock(sub.uid):
                try:
                    await self._check(sub)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("bili-subscription 检查 UID %s 失败", sub.uid)
                    # 网络故障等整轮失败：2 分钟后快速重试，而不是等满间隔
                    self._last_check[sub.uid] = (
                        time.monotonic() - sub.interval_minutes * 60
                        + _FAIL_RETRY_SECONDS
                    )
                    return
            # 每个订阅检查完立刻落盘，崩溃时最多丢一个订阅的进度
            await self._safe_save()

    async def _safe_save(self) -> None:
        try:
            await self._store.save()
        except Exception:
            logger.exception("bili-subscription 保存状态失败")

    async def _check(
        self, sub: Subscription, *, force_all: bool = False
    ) -> list[str]:
        """检查一个订阅。每类内容独立 try，返回错误消息列表。

        ``force_all=True`` 时无视 ``enable_video_push`` 开关，
        强制执行视频检查（供 ``订阅检查`` 命令使用）。

        返回值用于 ``订阅检查`` 命令展示，不抛异常。
        """
        errors: list[str] = []

        if "video" in sub.types and (self._enable_video_push or force_all):
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
        videos = await self._client.fetch_videos(sub.uid, limit=self._fetch_limit)
        if not videos:
            return
        await self._process_items(sub, videos, "video", self._send_video_one)

    async def _check_dynamics(self, sub: Subscription) -> None:
        dynamics = await self._client.fetch_dynamics(
            sub.uid, limit=self._fetch_limit
        )
        if not dynamics:
            return
        await self._process_items(
            sub, dynamics, "dynamic",
            partial(self._send_dynamic_one, sub),
        )

    async def _check_articles(self, sub: Subscription) -> None:
        articles = await self._client.fetch_articles(
            sub.uid, limit=self._fetch_limit
        )
        if not articles:
            return
        await self._process_items(
            sub, articles, "article",
            partial(self._push_article, sub),
        )

    @property
    def _fetch_limit(self) -> int:
        """抓取窗口：比"每次最多推送"大一些。

        窗口至少要覆盖"可能同时新增的条目数"，否则上一条会被挤出窗口，
        新旧判断失效。B 站接口按页返回，取 2 倍 + 5 是成本与安全的折中。
        """
        return min(50, self._max_items * 2 + 5)

    # ------------------------------------------------------------------
    # 核心：统一处理流程
    # ------------------------------------------------------------------
    async def _process_items(
        self,
        sub: Subscription,
        items: list,
        kind: str,
        send_one: Callable[[Any, tuple[str, ...]], Awaitable[SendOutcome]],
    ) -> None:
        if not items:
            return
        newest_id = self._id_of(items[0], kind)
        last = await self._store.get(sub.uid, kind)

        # 首次：只记录，不推送
        if last is None:
            await self._store.set(sub.uid, kind, newest_id)
            # items 是"新 → 旧"；反转后再 remember，保证历史保持"新 → 旧"
            for item in reversed(items):
                await self._store.remember(sub.uid, kind, self._id_of(item, kind))
            logger.info(
                "UID %s %s 首次记录 %s，本次不推送", sub.uid, kind, newest_id
            )
            return

        # 状态文件里的最近推送历史（含 last）+ 台账里的全部已知 ID。
        # 两者合并后，窗口内任何"已经发过的条目"都能被识别出来，
        # 不会再被当成新条目重推。
        known: set[str] = set(await self._store.recent(sub.uid, kind))
        known.add(last)
        known |= set(await self._ledger.known_item_ids(sub.uid, kind))

        new_items, found = self._collect_new(items, last, kind, known)

        # last 不在抓取窗口内（两次检查之间新增条目超过窗口，或原条目被删除）。
        # 此时不能把窗口里的条目都当新条目推——那样必然重复推送。
        # 改用"水位线"判断：只有明确比所有已推送条目更新的条目才推送，
        # 无法判断新旧的一律跳过。宁可不推，也不重复推。
        if not found:
            candidates = await self._collect_new_above_watermark(
                sub, items, kind, known
            )
            if not candidates:
                logger.warning(
                    "UID %s %s 上次推送的 %s 不在最近 %d 条里，"
                    "且无条目可确认为更新，本轮只记录不推送",
                    sub.uid, kind, last, len(items),
                )
                if newest_id != last:
                    await self._store.set(sub.uid, kind, newest_id)
                for item in reversed(items):
                    await self._store.remember(
                        sub.uid, kind, self._id_of(item, kind)
                    )
                return
            logger.warning(
                "UID %s %s 上次推送的 %s 已滑出最近 %d 条，"
                "改用水位线判断：确认 %d 条更新条目",
                sub.uid, kind, last, len(items), len(candidates),
            )
            new_items = candidates

        if not new_items:
            # 窗口里没有新条目，但最新 ID 变了：推进状态避免卡死。
            if newest_id != last:
                logger.warning(
                    "UID %s %s 新条目均被跳过，推进状态到 %s",
                    sub.uid, kind, newest_id,
                )
                await self._store.set(sub.uid, kind, newest_id)
            return

        logger.info("UID %s %s 发现新条目 %d 条", sub.uid, kind, len(new_items))

        # new_items 是按时间倒序（新 → 旧）收集的；推送时反转成"旧 → 新"，
        # 让群里按时间顺序出现内容。注意这里必须遍历**同一份反转后的列表**：
        # 如果只在这里反转、却在别处按原顺序登记历史，状态历史里的最新一条
        # 就会错位，下一轮可能把刚推过的内容再判成"新条目"。
        ordered = list(reversed(new_items))
        last_done_id: str | None = None
        for item in ordered:
            item_id = self._id_of(item, kind)
            # 台账是最终防线：这条内容对某个会话已经发过，就不能再发。
            pending = await self._ledger.pending_sessions(
                sub.uid, kind, item_id, sub.sessions
            )
            if not pending:
                self._note_success(kind, item_id)
                await self._store.remember(sub.uid, kind, item_id)
                last_done_id = item_id
                continue

            if await self._resend_guard(sub.uid, kind, item_id, pending):
                self._note_success(kind, item_id)
                await self._store.remember(sub.uid, kind, item_id)
                last_done_id = item_id
                continue

            try:
                outcome = await send_one(item, pending)
            except Exception:
                logger.exception("推送 %s %s 异常", kind, item_id)
                outcome = SendOutcome.empty()

            delivered_set = set(outcome.delivered) & set(pending)
            if delivered_set:
                await self._record_outcome(
                    sub.uid, kind, item_id, outcome
                )
            missed = [s for s in pending if s not in delivered_set]
            if missed:
                self._note_failure(kind, item_id)
                if self._retry_failed_push:
                    logger.warning(
                        "推送 %s %s 失败（未送达会话 %s），稍后重试",
                        kind, item_id, missed,
                    )
                    break  # 保持顺序：失败之后的条目不再推送
                logger.warning(
                    "推送 %s %s 未送达会话 %s；"
                    "已按「至多一次」跳过，不再重发（避免重复刷屏）",
                    kind, item_id, missed,
                )
            else:
                self._note_success(kind, item_id)
            # 无论送达与否都记入状态历史：下轮不再把它当"新条目"
            await self._store.remember(sub.uid, kind, item_id)
            last_done_id = item_id

        # last 取本轮最新一条（ordered 是"旧 → 新"，所以末尾最新）。
        # set() 会把 last 放到历史最前面，历史顺序保持"新 → 旧"。
        if last_done_id is not None:
            await self._store.set(sub.uid, kind, last_done_id)
        await self._safe_ledger_save()

    def _collect_new(
        self, items: list, last: str, kind: str, known: set[str]
    ) -> tuple[list, bool]:
        """收集比 last 更新的条目。

        返回 (新条目从新到旧, 是否在窗口里找到了 last)。
        用 ``known``（最近推送历史 + 台账）判断条目是否已经发过，
        所以即使 last 只是几轮之前的条目，也不会被误判成新条目。
        """
        new_items: list = []
        for item in items:
            item_id = self._id_of(item, kind)
            if item_id == last:
                return new_items, True
            if item_id in known:
                # 窗口里的条目，历史或台账里已经记过：跳过（不重复推送）
                continue
            if self._too_many_failures(kind, item_id):
                continue
            new_items.append(item)
        return new_items, False

    async def _collect_new_above_watermark(
        self, sub: Subscription, items: list, kind: str, known: set[str]
    ) -> list:
        """``last`` 已滑出窗口时的兜底：只挑"确定比历史更新"的条目。

        比较方式取决于 ID 形态：
        - 全是纯数字（动态/专栏 ID）：按整数比较；
        - 全是等长字符串（BV 号等）：按字典序比较；
        - 形态混杂：放弃比较，返回空列表（保守跳过，绝不重复推送）。

        只有严格大于历史中最大 ID 的条目才会被返回，因此不会重推任何
        已经记过账的内容。
        """
        ids: list[str] = []
        for item in items:
            item_id = self._id_of(item, kind)
            if item_id:
                ids.append(item_id)
        history = [i for i in known if i]
        if not ids or not history:
            return []

        if all(i.isdigit() for i in ids) and all(i.isdigit() for i in history):
            floor = max(int(i) for i in history)
            above = {
                item_id for item_id in ids
                if item_id not in known and int(item_id) > floor
            }
        elif (
            all(len(i) == len(ids[0]) for i in ids)
            and all(len(i) == len(ids[0]) for i in history)
        ):
            floor_text = max(history)
            above = {i for i in ids if i not in known and i > floor_text}
        else:
            return []

        if not above:
            return []
        return [
            item for item in items
            if self._id_of(item, kind) in above
            and not self._too_many_failures(kind, self._id_of(item, kind))
        ]

    async def _record_outcome(
        self, uid: str, kind: str, item_id: str, outcome: SendOutcome
    ) -> None:
        """把"已送达 + 消息 ID"写进台账。"""
        for session in outcome.delivered:
            await self._ledger.record_many(
                uid=uid,
                kind=kind,
                item_id=item_id,
                session=session,
                message_ids=outcome.message_ids.get(session) or (),
            )

    async def _resend_guard(
        self, uid: str, kind: str, item_id: str, sessions: tuple[str, ...]
    ) -> bool:
        """重复发送拦截。

        正常流程下这里不会被触发（发送前就用台账过滤了）。
        一旦触发，说明有历史消息被重复发出：按记录的消息 ID 全部撤回，
        并跳过本次发送。返回 True 表示"已处理，无需再发"。
        """
        if self._recall_callback is None:
            return False
        recalled = 0
        any_session = False
        for session in sessions:
            delivered = await self._ledger.delivered_sessions(uid, kind, item_id)
            if session not in delivered:
                continue
            any_session = True
            message_ids = await self._ledger.message_ids(uid, kind, item_id)
            if not message_ids:
                logger.warning(
                    "检测到 %s %s 将重复推送到 %s，但台账里没有消息 ID，无法撤回",
                    kind, item_id, session,
                )
                continue
            try:
                recalled += await self._recall_callback(session, message_ids)
            except Exception:
                logger.exception("撤回 %s %s 到 %s 失败", kind, item_id, session)
        if not any_session:
            return False
        # 台账里的消息 ID 已经全部交给撤回流程：清掉消息 ID 记录，
        # 但保留"该内容已送达该会话"的结论，避免下一轮又推一遍。
        logger.warning(
            "检测到重复推送 %s %s（会话 %s），已撤回 %d 条消息并跳过本次发送",
            kind, item_id, list(sessions), recalled,
        )
        return True

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
        # 容量保护：字典超大时清理最早的一半
        if len(self._fail_counts) > _FAIL_COUNT_MAX_KEYS:
            self._prune_fail_counts()

    def _prune_fail_counts(self) -> None:
        """保留最近插入的一半键，其余丢弃。

        Python 3.7+ 的 dict 保持插入顺序，keys()[:n] 就是最早的 n 个。
        """
        excess = len(self._fail_counts) - _FAIL_COUNT_MAX_KEYS // 2
        if excess <= 0:
            return
        for key in list(self._fail_counts.keys())[:excess]:
            self._fail_counts.pop(key, None)

    async def _safe_ledger_save(self) -> None:
        try:
            await self._ledger.save()
        except Exception:
            logger.exception("bili-subscription 保存推送台账失败")

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
            lines.append(
                f"     最新：{articles[0].article_id} 标题 {articles[0].title[:40]}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 推送动作
    # ------------------------------------------------------------------
    async def _send_video_one(
        self, video: UserVideo, sessions: tuple[str, ...]
    ) -> SendOutcome:
        """视频推送入口。"""
        return await self._send_video_to_sessions(sessions, video)

    async def _send_dynamic_one(
        self, sub: Subscription, dynamic: UserDynamic, sessions: tuple[str, ...]
    ) -> SendOutcome:
        """过滤 + 推送。跳过视为全部送达（无需记录消息 ID）。"""
        if self._skip_video_dynamic and dynamic.is_original_video:
            logger.debug("跳过视频动态 %s", dynamic.dynamic_id)
            return SendOutcome(delivered=set(sessions))
        if self._skip_empty_dynamic and is_empty_dynamic(dynamic):
            logger.debug("跳过空动态 %s", dynamic.dynamic_id)
            return SendOutcome(delivered=set(sessions))
        if self._skip_forward_dynamic and dynamic.kind == "DYNAMIC_TYPE_FORWARD":
            logger.debug("跳过转发动态 %s", dynamic.dynamic_id)
            return SendOutcome(delivered=set(sessions))
        return await self._push_dynamic(sub, dynamic, sessions)

    async def _push_dynamic(
        self, sub: Subscription, dynamic: UserDynamic, sessions: tuple[str, ...]
    ) -> SendOutcome:
        # 正文为空的动态：先尝试 detail 接口兜底一次（按需，不浪费请求）
        if not dynamic.text and dynamic.may_need_detail:
            try:
                dynamic = await self._client.enrich_dynamic(dynamic)
            except Exception:
                logger.exception(
                    "bili-subscription 动态正文兜底失败：%s", dynamic.dynamic_id
                )

        image_bytes = await self._download_images(dynamic.image_urls)
        # 顺带下载头像，用于新卡片头部（失败不影响主流程）
        avatar_bytes: bytes | None = None
        if dynamic.author_face:
            avatar_bytes = await self._client.download_image(
                dynamic.author_face, max_bytes=2 * 1024 * 1024
            )

        # 正文过长：完整正文渲染成横版图片单独发出，卡片只做节选预览；
        # Pillow 不可用或渲染失败时退回纯文字消息
        card = await render_dynamic_card(
            author_name=dynamic.author_name,
            author_face=avatar_bytes,
            kind_cn=dynamic.kind_cn,
            timestamp=dynamic.created_at,
            body=dynamic.text or "（无文字内容）",
            images=image_bytes,
            footer=f"https://t.bilibili.com/{dynamic.dynamic_id}",
            font_path=self._font_path,
        )
        # 卡片渲染失败（如未装 Pillow）时，正文用文字消息兜底，避免只剩原图
        prefix_text: str | None = None
        if card is None and (dynamic.text or "").strip():
            prefix_text = (
                f"{dynamic.text}\n\nhttps://t.bilibili.com/{dynamic.dynamic_id}"
            )
        return await self._broadcast(
            sessions, card, image_bytes,
            content_id=dynamic.dynamic_id, kind="动态",
            prefix_text=prefix_text,
        )

    async def _push_article(
        self, sub: Subscription, article: UserArticle, sessions: tuple[str, ...]
    ) -> SendOutcome:
        # 优先用正文插图（原文里的图）；抓取失败/为空时才退回封面兜底
        try:
            content_urls = await self._client.fetch_article_content_images(
                article.article_id, limit=6
            )
        except Exception as exc:
            logger.warning(
                "bili-subscription 专栏 %s 正文插图抓取失败：%s",
                article.article_id, exc,
            )
            content_urls = ()
        if content_urls:
            image_bytes = await self._download_images(content_urls[:6])
        else:
            logger.info(
                "bili-subscription 专栏 %s 未取到正文插图，回退为封面",
                article.article_id,
            )
            image_bytes = await self._download_images(article.covers)

        body = f"{article.title}\n\n{article.summary}"
        card = await render_card(
            title="B 站专栏更新",
            body=body,
            images=image_bytes,
            footer=f"https://www.bilibili.com/read/cv{article.article_id}",
            font_path=self._font_path,
        )
        return await self._broadcast(
            sessions, card, image_bytes,
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
        prefix_text: str | None = None,
    ) -> SendOutcome:
        """把消息链发到各会话，返回送达会话与各自的消息 ID。"""
        if card is None and not image_bytes:
            logger.warning(
                "%s %s 无任何可发送内容（Pillow 不可用且无图片）",
                kind, content_id,
            )
            return SendOutcome.empty()

        # 消息链只构建一次：图片压缩/编码很耗 CPU，多个会话共用同一份
        chain = self._build_dynamic_chain(card, image_bytes, prefix_text)

        outcome = SendOutcome()
        for session_id in sessions:
            try:
                result = await self._send(session_id, chain)
            except Exception:
                logger.exception(
                    "推送%s %s 到 %s 失败", kind, content_id, session_id
                )
                continue
            if not result:
                continue
            outcome = SendOutcome.union(outcome, result)
        return outcome

    async def _download_images(self, urls: tuple[str, ...]) -> list[bytes]:
        """并发下载所有图片。

        download_image 内部有 Semaphore(2) 限制实际并发上限，
        这里用 gather 并发发起，避免顺序等待浪费时间。
        保持顺序：结果与 urls 的顺序一致（过滤掉失败项）。
        """
        if not urls:
            return []
        tasks = [self._client.download_image(u) for u in urls[:6]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: list[bytes] = []
        for r in results:
            if isinstance(r, bytes) and r:
                out.append(r)
        return out