"""AstrBot B 站订阅推送插件入口。"""

from __future__ import annotations

import base64
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from yarl import URL

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain, Video
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

try:
    from astrbot.api.message_components import Node, Nodes
    _NODE_AVAILABLE = True
except ImportError:
    Node = None  # type: ignore[assignment]
    Nodes = None  # type: ignore[assignment]
    _NODE_AVAILABLE = False

from .bili_api import (
    BiliSubscriptionClient,
    UserVideo,
    _USER_AGENT,
    fetch_bili_ticket,
)
from .bili_player_bridge import (
    BiliVideoDownloader,
    has_bilibili_login,
    read_bilibili_cookies,
)
from .pusher import SubscriptionPusher, is_empty_dynamic, is_gif
from .render import pil_available, render_dynamic_card
from .subscription import (
    RuntimeSubStore,
    Subscription,
    SubscriptionStateStore,
    merge_subscriptions,
    parse_subscriptions,
    parse_types_text,
)


PLUGIN_NAME = "astrbot_plugin_bili_subscription"
BILI_PLAYER_PLUGIN_NAME = "astrbot_plugin_bili_player"

_PERMISSION_DENIED = (
    "该命令仅限管理员使用。\n"
    "如需授权，请联系机器人管理员在插件配置的“管理员白名单”里"
    "添加你的用户号或当前群号。"
)
_WHITELIST_EMPTY = (
    "管理员白名单为空，所有管理命令已被禁用。\n"
    "请在插件配置的“管理员白名单”里添加管理员用户号或群号。"
)
_SUBSCRIBE_USAGE = (
    "用法：订阅 <B站UID> [群号] [推送类型] [间隔分钟]\n"
    "示例：\n"
    "· 订阅 2267573 —— 订阅到当前会话，使用默认类型与间隔\n"
    "· 订阅 2267573 123456789 —— 指定推送到群 123456789\n"
    "· 订阅 2267573 视频,动态 30 —— 只推视频和动态，每 30 分钟检查\n"
    "· 类型可选：video(视频)、dynamic(动态)、article(专栏)\n"
    "退订：退订 <B站UID> 移除当前会话；退订 <B站UID> all 全部移除"
)


@dataclass(frozen=True, slots=True)
class _AdminWhitelist:
    any_ids: frozenset[str] = field(default_factory=frozenset)
    user_ids: frozenset[str] = field(default_factory=frozenset)
    group_ids: frozenset[str] = field(default_factory=frozenset)

    @property
    def is_empty(self) -> bool:
        return not (self.any_ids or self.user_ids or self.group_ids)

    def allows(self, *, user_id: str | None, group_id: str | None) -> bool:
        if self.is_empty:
            return False
        if user_id and (user_id in self.any_ids or user_id in self.user_ids):
            return True
        if group_id and (group_id in self.any_ids or group_id in self.group_ids):
            return True
        return False


def _parse_whitelist(raw: object) -> _AdminWhitelist:
    if raw is None:
        return _AdminWhitelist()
    text = (
        str(raw)
        .replace("，", ",").replace("；", ",").replace(";", ",")
        .replace("\r", "\n").replace("\n", ",")
    )
    any_ids: set[str] = set()
    user_ids: set[str] = set()
    group_ids: set[str] = set()
    for piece in text.split(","):
        token = piece.strip()
        if not token:
            continue
        lowered = token.casefold()
        if lowered.startswith("u:") and token[2:].strip().isdigit():
            user_ids.add(token[2:].strip())
        elif lowered.startswith("g:") and token[2:].strip().isdigit():
            group_ids.add(token[2:].strip())
        elif token.isdigit():
            any_ids.add(token)
    return _AdminWhitelist(
        any_ids=frozenset(any_ids),
        user_ids=frozenset(user_ids),
        group_ids=frozenset(group_ids),
    )


# ----------------------------------------------------------------------
# B 站初始化：预热 + bili_ticket
# ----------------------------------------------------------------------
async def _warmup_bilibili(
    session: aiohttp.ClientSession, cache_path: Path
) -> None:
    """预访问 B 站首页，让 CookieJar 拿到 buvid3 / b_nut 等基础 Cookie，
    再补一个 bili_ticket 完善指纹。
    """
    target_url = URL("https://www.bilibili.com/")

    if cache_path.exists():
        try:
            saved = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(saved, dict) and saved:
                session.cookie_jar.update_cookies(
                    saved, response_url=target_url
                )
                logger.info(
                    "bili-subscription 已恢复 %d 个历史 Cookie", len(saved)
                )
        except Exception as exc:
            logger.debug("bili-subscription 读取 buvid 缓存失败：%s", exc)

    try:
        async with session.get(
            "https://www.bilibili.com/",
            headers={"User-Agent": _USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            # 只需要响应头里的 Set-Cookie，不下载整页 HTML，省流量省时间
            await resp.release()
    except Exception as exc:
        logger.warning("bili-subscription 预访问首页失败（不影响运行）：%s", exc)

    fresh: dict[str, str] = {}
    try:
        jar = session.cookie_jar.filter_cookies(target_url)
        for name, morsel in jar.items():
            if name.startswith("buvid") or name in ("b_nut", "_uuid"):
                fresh[name] = morsel.value
    except Exception as exc:
        logger.debug("bili-subscription 读取 CookieJar 失败：%s", exc)

    # 补充 bili_ticket
    ticket_cookies = await fetch_bili_ticket(session)
    if ticket_cookies:
        try:
            session.cookie_jar.update_cookies(
                ticket_cookies, response_url=target_url
            )
            fresh.update(ticket_cookies)
            logger.info(
                "bili-subscription 已获取 bili_ticket（%d 字符）",
                len(ticket_cookies.get("bili_ticket", "")),
            )
        except Exception as exc:
            logger.debug("bili-subscription 写入 bili_ticket 失败：%s", exc)

    if fresh:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps(fresh, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(
                "bili-subscription 已缓存设备标识：%s",
                ", ".join(sorted(fresh.keys())),
            )
        except Exception as exc:
            logger.debug("bili-subscription 保存缓存失败：%s", exc)
    else:
        logger.warning(
            "bili-subscription 预访问首页未拿到 buvid / bili_ticket，"
            "可能是网络问题或被风控，稍后会自动重试"
        )


# ----------------------------------------------------------------------
# 视频信息格式化
# ----------------------------------------------------------------------
def _format_duration(seconds: int) -> str:
    if seconds <= 0:
        return ""
    m, s = divmod(seconds, 60)
    if m > 0:
        return f"{m} 分 {s} 秒"
    return f"{s} 秒"


def _clip(text: str, limit: int) -> str:
    """截断过长文本，防止消息超长。"""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _parse_hhmm(raw: object) -> int | None:
    """解析 HH:MM 为当日分钟数；非法返回 None。"""
    value = str(raw or "").strip()
    try:
        hour_text, minute_text = value.split(":", 1)
        total = int(hour_text) * 60 + int(minute_text)
    except ValueError:
        return None
    return total if 0 <= total < 1440 else None


def _fmt_hm(total: int | None) -> str:
    if total is None:
        return "--:--"
    return f"{total // 60:02d}:{total % 60:02d}"


def _format_video_caption(
    video: UserVideo,
    *,
    page_count: int = 1,
    page_title: str = "",
    title: str | None = None,
    uploader: str | None = None,
) -> str:
    """生成视频消息的文本头部。

    信息完整，用户不点链接也知道是谁发的。
    title / uploader 可传入下载时拿到的权威值（覆盖列表接口的结果）。
    page_count > 1 时附加"多 P 视频仅推送第 1 P"提示。
    """
    title_text = title or video.title
    uploader_text = uploader or video.uploader
    lines: list[str] = [
        f"【视频更新】{_clip(title_text or '（无标题）', 100)}"
    ]
    if uploader_text:
        lines.append(f"UP主：{_clip(uploader_text, 40)}")
    duration = _format_duration(video.duration_seconds)
    if duration:
        lines.append(f"时长：{duration}")
    if video.view_count > 0:
        lines.append(f"播放：{video.view_count}")
    if page_count > 1:
        first_title = _clip(page_title or "P1", 40)
        lines.append(
            f"分 P：共 {page_count} P，仅推送第 1 P「{first_title}」"
        )
    lines.append(f"https://www.bilibili.com/video/{video.bvid}")
    return "\n".join(lines)


# ----------------------------------------------------------------------
# 插件主体
# ----------------------------------------------------------------------
class BiliSubscriptionPlugin(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None) -> None:
        super().__init__(context, config)
        self._config = dict(config or {})
        self._http: aiohttp.ClientSession | None = None
        self._client: BiliSubscriptionClient | None = None
        self._store: SubscriptionStateStore | None = None
        self._runtime_store: RuntimeSubStore | None = None
        self._pusher: SubscriptionPusher | None = None
        self._downloader: BiliVideoDownloader | None = None
        self._bili_player_data_dir: Path | None = None
        self._initialized = False
        self._last_subs_signature: tuple[str, ...] = ()
        # 视频发送大小上限（字节）；0 表示不限制
        self._max_video_send_bytes = 0

    # ------------------------------------------------------------------
    # 配置
    # ------------------------------------------------------------------
    def _refresh_config_from_astrbot(self) -> None:
        """从 AstrBot 重新读取插件配置。"""
        candidates: list[object] = []

        star_cfg = getattr(self, "config", None)
        if isinstance(star_cfg, dict):
            nested = star_cfg.get(PLUGIN_NAME)
            if isinstance(nested, dict):
                candidates.append(nested)
            candidates.append(star_cfg)

        try:
            live = self.context.get_config()
            if isinstance(live, dict):
                section = live.get(PLUGIN_NAME)
                if isinstance(section, dict):
                    candidates.append(section)
                nested = live.get("plugin")
                if isinstance(nested, dict):
                    section = nested.get(PLUGIN_NAME)
                    if isinstance(section, dict):
                        candidates.append(section)
        except Exception:
            pass

        for candidate in candidates:
            if isinstance(candidate, dict) and candidate:
                self._config.update(candidate)
                return

    def _refresh_config_and_subs(self) -> None:
        """刷新配置、开关和订阅列表。无需重启即可生效。"""
        self._refresh_config_from_astrbot()

        # 视频发送大小上限
        try:
            mb = int(self._config.get("max_video_send_mb") or 0)
            self._max_video_send_bytes = mb * 1024 * 1024 if mb > 0 else 0
        except (TypeError, ValueError):
            self._max_video_send_bytes = 0

        # 视频清晰度热重载
        if self._client is not None:
            try:
                self._client.set_video_quality(
                    int(self._config.get("video_quality") or 32)
                )
            except (TypeError, ValueError):
                self._client.set_video_quality(32)

        quiet_enabled, quiet_start, quiet_end = self._quiet_options()
        if self._pusher is not None:
            try:
                self._pusher.update_options(
                    enable_video_push=bool(
                        self._config.get("enable_video_push", True)
                    ),
                    skip_video_dynamic=bool(
                        self._config.get("skip_video_dynamic", True)
                    ),
                    skip_empty_dynamic=bool(
                        self._config.get("skip_empty_dynamic", True)
                    ),
                    skip_forward_dynamic=bool(
                        self._config.get("skip_forward_dynamic", True)
                    ),
                    max_items_per_push=int(
                        self._config.get("max_items_per_push") or 5
                    ),
                    scan_interval_seconds=int(
                        self._config.get("scan_interval_seconds") or 60
                    ),
                    font_path=str(
                        self._config.get("image_font_path") or ""
                    ) or None,
                    quiet_enabled=quiet_enabled,
                    quiet_start_minutes=quiet_start,
                    quiet_end_minutes=quiet_end,
                )
            except Exception:
                logger.exception("bili-subscription 刷新开关失败")

        subs, errors = self._parse_subs()
        if any(e.startswith("订阅配置解析失败") for e in errors):
            # 整体解析失败：保持现有订阅不动，避免一次异常把订阅清空
            logger.warning("订阅配置解析失败，保持现有订阅不变")
            return
        signature = self._subs_signature(subs)
        if signature != self._last_subs_signature:
            self._last_subs_signature = signature
            if self._pusher is not None:
                self._pusher.update_subscriptions(subs)
                logger.info("订阅列表已重载：%d 条", len(subs))
        for error in errors:
            logger.warning("订阅配置错误：%s", error)

    def _detect_default_adapter(self) -> str:
        configured = str(self._config.get("platform_adapter") or "").strip()
        if configured:
            return configured
        try:
            config = self.context.get_config()
            platforms = config.get("platform") if hasattr(config, "get") else None
            if isinstance(platforms, list):
                for platform in platforms:
                    if not isinstance(platform, dict):
                        continue
                    if not platform.get("enable", True):
                        continue
                    pid = platform.get("id")
                    if isinstance(pid, str) and pid.strip():
                        return pid.strip()
        except Exception:
            pass
        return "default"

    def _default_types_set(self) -> frozenset[str]:
        text = str(self._config.get("default_types") or "video,dynamic,article")
        return parse_types_text(text) or frozenset({"video", "dynamic", "article"})

    def _default_interval_minutes(self) -> int:
        try:
            return max(1, int(self._config.get("default_interval_minutes") or 15))
        except (TypeError, ValueError):
            return 15

    def _quiet_options(self) -> tuple[bool, int | None, int | None]:
        start = _parse_hhmm(self._config.get("quiet_start"))
        end = _parse_hhmm(self._config.get("quiet_end"))
        enabled = (
            bool(self._config.get("quiet_hours_enabled", False))
            and start is not None
            and end is not None
            and start != end
        )
        return enabled, start, end

    def _parse_subs(self) -> tuple[list[Subscription], list[str]]:
        try:
            subs, errors = parse_subscriptions(
                self._config.get("subscriptions"),
                default_adapter=self._detect_default_adapter(),
                default_types=str(
                    self._config.get("default_types") or "video,dynamic,article"
                ).strip() or "video,dynamic,article",
                default_interval_minutes=self._default_interval_minutes(),
            )
            # 合并聊天命令（订阅/退订）添加的运行时订阅
            if self._runtime_store is not None and self._runtime_store.count:
                subs = merge_subscriptions(
                    subs, self._runtime_store.to_subscriptions()
                )
            return subs, errors
        except Exception as exc:
            logger.exception("bili-subscription 解析订阅配置失败")
            return [], [f"订阅配置解析失败：{exc}"]

    @staticmethod
    def _subs_signature(subs: list[Subscription]) -> tuple[str, ...]:
        return tuple(
            sorted(
                f"{s.uid}|{','.join(s.sessions)}|"
                f"{','.join(sorted(s.types))}|{s.interval_minutes}"
                for s in subs
            )
        )

    def _whitelist(self) -> _AdminWhitelist:
        return _parse_whitelist(self._config.get("admin_whitelist"))

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        return self._whitelist().allows(
            user_id=_event_str(event, "get_sender_id"),
            group_id=_event_str(event, "get_group_id"),
        )

    def _deny_reply(self) -> str:
        return _WHITELIST_EMPTY if self._whitelist().is_empty else _PERMISSION_DENIED

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return

        data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        data_dir.mkdir(parents=True, exist_ok=True)
        bili_player_dir = StarTools.get_data_dir(BILI_PLAYER_PLUGIN_NAME)

        connector = aiohttp.TCPConnector(limit=8, limit_per_host=4, ttl_dns_cache=300)
        http = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30, connect=10, sock_read=20),
        )

        try:
            await _warmup_bilibili(http, data_dir / "buvid_cache.json")

            try:
                min_gap = float(
                    self._config.get("min_request_gap_seconds") or 1.5
                )
            except (TypeError, ValueError):
                min_gap = 1.5
            try:
                video_quality = int(self._config.get("video_quality") or 32)
            except (TypeError, ValueError):
                video_quality = 32

            client = BiliSubscriptionClient(
                http,
                credentials_getter=lambda: read_bilibili_cookies(bili_player_dir),
                min_request_gap=min_gap,
                video_quality=video_quality,
            )
            store = SubscriptionStateStore(data_dir / "subscription_state.json")
            await store.load()

            runtime_store = RuntimeSubStore(data_dir / "runtime_subscriptions.json")
            await runtime_store.load()
            self._runtime_store = runtime_store

            downloader = BiliVideoDownloader(client=client, data_dir=data_dir)
            if downloader.ffmpeg_available:
                logger.info("bili-subscription 视频下载器已就绪")
            else:
                logger.warning("bili-subscription 未检测到 ffmpeg，视频推送将被跳过")

            if has_bilibili_login(bili_player_dir):
                logger.info("bili-subscription 检测到 B 站登录状态")
            else:
                logger.warning(
                    "bili-subscription 未检测到 B 站登录 —— "
                    "未登录时 B 站风控极严，动态/专栏接口可能返回空或频繁限流。"
                    "强烈建议在 astrbot_plugin_bili_player 的插件 Page 里扫码登录。"
                )

            # 视频下载预算
            try:
                mb = int(self._config.get("max_video_send_mb") or 0)
                self._max_video_send_bytes = mb * 1024 * 1024 if mb > 0 else 0
            except (TypeError, ValueError):
                self._max_video_send_bytes = 0
            if self._max_video_send_bytes > 0:
                logger.info(
                    "bili-subscription 视频下载预算：%d MB（超过则降级为封面卡片）",
                    self._max_video_send_bytes // (1024 * 1024),
                )
            else:
                logger.info(
                    "bili-subscription 视频下载预算：不限制"
                    "（底层硬上限 %d MB）",
                    BiliVideoDownloader.DEFAULT_PROGRESSIVE_HARD_CAP
                    // (1024 * 1024),
                )

            quiet_enabled, quiet_start, quiet_end = self._quiet_options()
            pusher = SubscriptionPusher(
                client=client,
                store=store,
                send_callback=self._send_chain,
                build_dynamic_chain=self._build_dynamic_chain,
                send_video_to_sessions=self._send_video_to_sessions,
                max_items_per_push=int(self._config.get("max_items_per_push") or 5),
                font_path=str(self._config.get("image_font_path") or "") or None,
                scan_interval_seconds=int(
                    self._config.get("scan_interval_seconds") or 60
                ),
                enable_video_push=bool(self._config.get("enable_video_push", True)),
                skip_video_dynamic=bool(
                    self._config.get("skip_video_dynamic", True)
                ),
                skip_empty_dynamic=bool(
                    self._config.get("skip_empty_dynamic", True)
                ),
                skip_forward_dynamic=bool(
                    self._config.get("skip_forward_dynamic", True)
                ),
                config_refresher=self._refresh_config_and_subs,
                max_concurrent_checks=int(
                    self._config.get("max_concurrent_checks") or 2
                ),
                quiet_enabled=quiet_enabled,
                quiet_start_minutes=quiet_start,
                quiet_end_minutes=quiet_end,
            )

            subs, errors = self._parse_subs()
            for error in errors:
                logger.warning("bili-subscription 配置错误：%s", error)
            pusher.update_subscriptions(subs)
            self._last_subs_signature = self._subs_signature(subs)

            self._http = http
            self._client = client
            self._store = store
            self._pusher = pusher
            self._downloader = downloader
            self._bili_player_data_dir = bili_player_dir
            self._initialized = True
            pusher.start()
            logger.info(
                "bili-subscription initialized with %d subscription(s)", len(subs)
            )
        except Exception:
            await http.close()
            raise

    async def terminate(self) -> None:
        self._initialized = False
        pusher, downloader, http = self._pusher, self._downloader, self._http
        self._pusher = None
        self._downloader = None
        self._client = None
        self._store = None
        self._runtime_store = None
        self._http = None
        self._bili_player_data_dir = None
        if pusher is not None:
            await pusher.stop()
        if downloader is not None:
            try:
                await downloader.aclose()
            except Exception:
                logger.exception("bili-subscription 关闭视频下载器失败")
        if http is not None and not http.closed:
            await http.close()

    # ------------------------------------------------------------------
    # 消息构造
    # ------------------------------------------------------------------
    async def _send_chain(self, session_id: str, chain: Any) -> bool:
        try:
            if isinstance(chain, _SplitChain):
                sent = False
                if chain.first is not None:
                    try:
                        await self.context.send_message(session_id, chain.first)
                        sent = True
                    except Exception:
                        logger.exception(
                            "bili-subscription 向 %s 发送卡片失败", session_id
                        )
                if chain.forward is not None:
                    try:
                        await self.context.send_message(session_id, chain.forward)
                        sent = True
                    except Exception:
                        logger.warning(
                            "bili-subscription 合并转发失败（已忽略）：%s",
                            session_id,
                        )
                # 只要发出了一部分就算成功，避免"卡片已送达却被标记失败"
                # 导致下一轮重复推送同一张卡片
                return sent
            await self.context.send_message(session_id, chain)
            return True
        except Exception:
            logger.exception("bili-subscription 向 %s 推送失败", session_id)
            return False

    def _build_dynamic_chain(
        self, card_bytes: bytes | None, images: list[bytes]
    ) -> Any:
        """组装消息链。

        - 卡片合成图打头（JPEG，动图在这里只显示第一帧）；
        - 原图放进合并转发（聊天记录）：静态图压缩成 JPEG，
          GIF 动图用原图字节发送以保留动画。
        """
        first_chain = (
            MessageChain([_image_from_bytes(card_bytes)]) if card_bytes else None
        )

        forward_chain: MessageChain | None = None
        if (
            bool(self._config.get("enable_node_forward", True))
            and _NODE_AVAILABLE
            and Node is not None and Nodes is not None
            and images
        ):
            nodes: list[Any] = []
            for index, img in enumerate(images[:6], start=1):
                if is_gif(img):
                    label = f"动图 {index}"
                    component = _image_from_bytes(img)  # 原样，保留动画
                elif len(img) <= _FORWARD_IMAGE_MAX_BYTES:
                    label = f"原图 {index}"
                    component = _image_from_bytes(img)  # 原图直发，保证文字清晰可读
                else:
                    label = f"原图 {index}"
                    component = _image_from_bytes_compressed(img)
                nodes.append(
                    Node(
                        content=[Plain(label), component],
                        name="B站更新", uin="10000",
                    )
                )
            if nodes:
                forward_chain = MessageChain([Nodes(nodes)])

        return _SplitChain(first=first_chain, forward=forward_chain)

    # ------------------------------------------------------------------
    # 视频发送（含降级）
    # ------------------------------------------------------------------
    async def _send_video_to_sessions(
        self, sessions: tuple[str, ...], video: UserVideo
    ) -> set[str]:
        """下载视频并发送，返回成功送达的会话集合。

        以下情况降级为"视频卡片"：
        ① 下载器不可用（没装 ffmpeg）；
        ② 视频下载失败（含预算超限，已在底层借助 Content-Length 提前放弃）；
        ③ 下载后文件不存在 / 为空。

        下载预算（max_video_send_mb）通过 max_total_bytes 一路传到下载器，
        由下载器动态切给视频轨/音频轨，并在 HTTP 层提前判断是否放弃。
        """
        if self._downloader is None or not self._downloader.ffmpeg_available:
            logger.warning("bili-subscription 视频下载器不可用，降级为卡片：%s", video.bvid)
            return await self._send_video_fallback(
                sessions, video, reason="视频下载器不可用（未装 ffmpeg）"
            )

        try:
            downloaded = await self._downloader.download(
                video.bvid,
                max_total_bytes=self._max_video_send_bytes,
            )
        except Exception:
            # 下载过程抛异常（如接口风控）：不能静默失败，降级为卡片告知用户
            logger.exception(
                "bili-subscription 视频下载异常，降级为卡片：%s", video.bvid
            )
            return await self._send_video_fallback(
                sessions, video,
                reason="视频下载异常，请点击链接前往 B 站观看",
            )
        if downloaded is None:
            logger.warning(
                "bili-subscription 视频下载失败或超预算，降级为卡片：%s",
                video.bvid,
            )
            return await self._send_video_fallback(
                sessions, video,
                reason="视频下载失败或超出大小预算，请点击链接前往 B 站观看",
            )

        try:
            # 兜底检查：正常情况下下载器已按预算控制大小
            try:
                size = downloaded.path.stat().st_size
            except OSError:
                logger.warning(
                    "bili-subscription 视频文件不存在，降级为卡片：%s", video.bvid
                )
                return await self._send_video_fallback(
                    sessions, video, reason="视频文件丢失"
                )

            if size <= 0:
                logger.warning(
                    "bili-subscription 视频文件为空，降级为卡片：%s", video.bvid
                )
                return await self._send_video_fallback(
                    sessions, video, reason="视频文件为空"
                )

            # 文本头部带标题、UP主、时长、播放量；多 P 视频加提示
            caption = _format_video_caption(
                video,
                page_count=downloaded.page_count,
                page_title=downloaded.page_title,
                title=downloaded.title,
                uploader=downloaded.uploader,
            )
            delivered: set[str] = set()
            for session_id in sessions:
                try:
                    # 先发文字说明（标题/作者/链接），再发视频文件，
                    # 避免部分平台把文字和视频塞在同一条消息里时丢掉文字
                    await self.context.send_message(
                        session_id, MessageChain([Plain(caption)])
                    )
                    component = Video.fromFileSystem(str(downloaded.path))
                    await self.context.send_message(
                        session_id, MessageChain([component])
                    )
                    delivered.add(session_id)
                except Exception:
                    logger.exception(
                        "bili-subscription 发送视频 %s 到 %s 失败",
                        video.bvid, session_id,
                    )
            return delivered
        finally:
            try:
                await downloaded.release()
            except Exception:
                logger.exception("bili-subscription 释放视频文件失败")

    async def _send_video_fallback(
        self, sessions: tuple[str, ...], video: UserVideo, *, reason: str
    ) -> set[str]:
        """视频无法发送时的降级：推送一张含封面、标题、UP 主、链接的卡片。

        返回成功送达的会话集合。
        """
        # 下载封面
        cover_bytes: bytes | None = None
        if video.cover and self._client is not None:
            cover_bytes = await self._client.download_image(
                video.cover, max_bytes=4 * 1024 * 1024
            )

        # 拼正文（信息尽量完整）
        body_lines: list[str] = [video.title or "（无标题）"]
        body_lines.append("")
        if video.uploader:
            body_lines.append(f"UP主：{video.uploader}")
        duration = _format_duration(video.duration_seconds)
        if duration:
            body_lines.append(f"时长：{duration}")
        if video.view_count > 0:
            body_lines.append(f"播放：{video.view_count}")
        body_lines.append(f"链接：https://www.bilibili.com/video/{video.bvid}")

        card = await render_dynamic_card(
            author_name=video.uploader or "B站UP主",
            author_face=None,  # vlist 里没有头像 URL，不额外请求
            kind_cn="视频更新",
            timestamp=video.created_at,
            body="\n".join(body_lines),
            images=[cover_bytes] if cover_bytes else [],
            footer=f"⚠️ {reason}",
            font_path=str(self._config.get("image_font_path") or "") or None,
        )

        # Pillow 不可用 → 退化为纯文本
        if card is None:
            text = _format_video_caption(video) + f"\n（{reason}）"
            delivered: set[str] = set()
            for session_id in sessions:
                try:
                    await self.context.send_message(
                        session_id, MessageChain([Plain(text)])
                    )
                    delivered.add(session_id)
                except Exception:
                    logger.exception(
                        "bili-subscription 发送视频卡片（纯文本）到 %s 失败", session_id
                    )
            return delivered

        # 正常发送卡片
        delivered = set()
        for session_id in sessions:
            try:
                await self.context.send_message(
                    session_id, MessageChain([_image_from_bytes(card)])
                )
                delivered.add(session_id)
            except Exception:
                logger.exception(
                    "bili-subscription 发送视频卡片到 %s 失败", session_id
                )
        return delivered

    # ------------------------------------------------------------------
    # 命令：订阅 / 退订（聊天内管理订阅，无需进 WebUI）
    # ------------------------------------------------------------------
    def _current_session(self, event: AstrMessageEvent) -> str:
        """当前会话的会话 ID；拿不到时退回 适配器:GroupMessage:群号。"""
        session_id = str(event.unified_msg_origin or "").strip()
        if session_id:
            return session_id
        group_id = _event_str(event, "get_group_id")
        if group_id:
            return f"{self._detect_default_adapter()}:GroupMessage:{group_id}"
        return ""

    @filter.command("订阅")
    async def cmd_subscribe(self, event: AstrMessageEvent, arg: str = ""):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return
        if self._runtime_store is None:
            yield event.plain_result("插件未初始化")
            event.stop_event()
            return

        self._refresh_config_and_subs()
        tokens = (arg or "").split()
        if not tokens or not tokens[0].isdigit():
            yield event.plain_result(_SUBSCRIBE_USAGE)
            event.stop_event()
            return

        uid = tokens[0]
        rest = tokens[1:]

        # 第 2 个参数：纯数字或完整会话 ID 视为目标群
        group_arg: str | None = None
        if rest and (rest[0].isdigit() or ":" in rest[0]):
            group_arg = rest.pop(0)

        # 其余参数：纯数字 → 间隔分钟；其他 → 推送类型
        type_parts: list[str] = []
        interval: int | None = None
        for token in rest:
            if token.isdigit():
                interval = max(1, min(1440, int(token)))
            else:
                type_parts.append(token)

        types = parse_types_text(",".join(type_parts)) if type_parts else None
        if type_parts and types is None:
            yield event.plain_result(
                f"推送类型无效：{','.join(type_parts)}\n"
                "可选 video(视频)、dynamic(动态)、article(专栏)。"
            )
            event.stop_event()
            return

        adapter = self._detect_default_adapter()
        if group_arg:
            session_id = (
                group_arg if ":" in group_arg
                else f"{adapter}:GroupMessage:{group_arg}"
            )
        else:
            session_id = self._current_session(event)
            if not session_id:
                yield event.plain_result(
                    "无法识别当前会话，请显式指定群号：\n订阅 <uid> <群号> [类型] [间隔分钟]"
                )
                event.stop_event()
                return

        # 去重检查（配置订阅与运行时订阅合并后）
        subs, _ = self._parse_subs()
        existing = next(
            (s for s in subs if s.uid == uid and session_id in s.sessions), None
        )
        if existing is not None:
            yield event.plain_result(
                f"UID {uid} 已订阅到该会话"
                f"（类型 {','.join(sorted(existing.types))}，"
                f"间隔 {existing.interval_minutes} 分钟），无需重复添加。\n"
                "修改配置订阅请到 WebUI；移除本命令添加的订阅可用：退订 " + uid
            )
            event.stop_event()
            return

        sub = Subscription(
            uid=uid,
            sessions=(session_id,),
            types=types or self._default_types_set(),
            interval_minutes=interval or self._default_interval_minutes(),
        )
        self._runtime_store.upsert(sub)
        await self._runtime_store.save()
        self._refresh_config_and_subs()
        yield event.plain_result(
            f"✅ 已订阅 UID {uid}\n"
            f"推送到：{session_id}\n"
            f"类型：{','.join(sorted(sub.types))}\n"
            f"检查间隔：{sub.interval_minutes} 分钟\n"
            "首次检查只记录当前最新，之后有新内容才会推送。\n"
            "退订：退订 " + uid
        )
        event.stop_event()

    @filter.command("退订")
    async def cmd_unsubscribe(self, event: AstrMessageEvent, arg: str = ""):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return
        if self._runtime_store is None:
            yield event.plain_result("插件未初始化")
            event.stop_event()
            return

        self._refresh_config_and_subs()
        tokens = (arg or "").split()
        if not tokens or not tokens[0].isdigit():
            yield event.plain_result(
                "用法：退订 <B站UID> [all]\n"
                "默认只移除当前会话的运行时订阅；加 all 移除该 UID 的全部运行时订阅。"
            )
            event.stop_event()
            return

        uid = tokens[0]
        remove_all = len(tokens) > 1 and tokens[1].casefold() == "all"
        session_id = self._current_session(event)

        entries = self._runtime_store.find(uid)
        if not entries:
            subs, _ = self._parse_subs()
            if any(s.uid == uid for s in subs):
                yield event.plain_result(
                    f"UID {uid} 的订阅来自插件配置，"
                    "请在 WebUI 的「订阅列表」里删除。"
                )
            else:
                yield event.plain_result(f"未找到 UID {uid} 的运行时订阅。")
            event.stop_event()
            return

        if not remove_all:
            matched = [e for e in entries if session_id and session_id in e.sessions]
            if not matched:
                targets = "、".join(",".join(e.sessions) for e in entries)
                yield event.plain_result(
                    f"UID {uid} 的运行时订阅不在当前会话（目标：{targets}）。\n"
                    f"如需全部移除：退订 {uid} all"
                )
                event.stop_event()
                return

        removed = self._runtime_store.remove(uid, session=None if remove_all else session_id)
        await self._runtime_store.save()
        self._refresh_config_and_subs()

        subs, _ = self._parse_subs()
        note = ""
        if any(s.uid == uid for s in subs):
            note = "\n注意：该 UID 在插件配置中仍有订阅，如不需要请在 WebUI 删除。"
        yield event.plain_result(
            f"✅ 已移除 {len(removed)} 条运行时订阅：UID {uid}{note}"
        )
        event.stop_event()

    # ------------------------------------------------------------------
    # 命令：订阅状态
    # ------------------------------------------------------------------
    @filter.command("订阅状态")
    async def cmd_status(self, event: AstrMessageEvent):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return

        self._refresh_config_and_subs()
        subs, errors = self._parse_subs()

        if self._bili_player_data_dir is None:
            login_state = "未检测到原插件"
        elif has_bilibili_login(self._bili_player_data_dir):
            login_state = "已登录"
        else:
            login_state = "未登录"

        lines: list[str] = []
        if not subs and not errors:
            lines.append("当前没有任何订阅。")
            lines.append("请在插件配置的“订阅列表”里点“添加”，填上 B站UID 和 群号。")
        else:
            lines.append(f"当前共 {len(subs)} 条订阅：")
            for s in subs:
                lines.append(
                    f"- UID {s.uid}：推送到 {len(s.sessions)} 个会话；"
                    f"类型 {','.join(sorted(s.types))}；"
                    f"间隔 {s.interval_minutes} 分钟"
                )
            if errors:
                lines.append("")
                lines.append("配置错误：")
                lines.extend(f"- {e}" for e in errors)

        lines.append("")
        lines.append(f"平台实例前缀（自动检测）：{self._detect_default_adapter()}")
        lines.append(f"B 站登录状态：{login_state}")
        if login_state != "已登录":
            lines.append(
                "⚠️ 请在 astrbot_plugin_bili_player 的插件 Page “账号与运行状态”里"
                "扫码登录或手动粘贴 Cookie。"
            )

        if self._downloader is None:
            lines.append("")
            lines.append("⚠️ 视频下载器不可用。")
        elif not self._downloader.ffmpeg_available:
            lines.append("")
            lines.append("⚠️ 未检测到 ffmpeg，视频推送将被跳过。")

        lines.append("")
        if self._max_video_send_bytes > 0:
            lines.append(
                f"视频下载预算：{self._max_video_send_bytes // (1024 * 1024)} MB"
                "（超过则降级为卡片）"
            )
        else:
            lines.append(
                f"视频下载预算：不限制（硬上限 "
                f"{BiliVideoDownloader.DEFAULT_PROGRESSIVE_HARD_CAP // (1024 * 1024)}"
                " MB）"
            )

        lines.append(
            "视频动态过滤："
            f"{'已开启' if self._config.get('skip_video_dynamic', True) else '已关闭'}"
        )
        lines.append(
            "空动态过滤："
            f"{'已开启' if self._config.get('skip_empty_dynamic', True) else '已关闭'}"
        )
        lines.append(
            "转发动态过滤："
            f"{'已开启' if self._config.get('skip_forward_dynamic', True) else '已关闭'}"
        )

        runtime_count = (
            self._runtime_store.count if self._runtime_store is not None else 0
        )
        lines.append(f"运行时订阅（聊天命令添加）：{runtime_count} 条，可用「订阅 / 退订」管理")
        quiet_enabled, quiet_start, quiet_end = self._quiet_options()
        if quiet_enabled:
            lines.append(
                f"静音时段：已开启（{_fmt_hm(quiet_start)} - {_fmt_hm(quiet_end)}，"
                "时段内暂停检查，结束后自动补推）"
            )
        else:
            lines.append("静音时段：未开启")

        if self._client is not None:
            if self._client.in_cooldown():
                lines.append(
                    "⚠️ 风控冷却中：剩余约 "
                    f"{self._client.cooldown_remaining_seconds() // 60 + 1} 分钟，"
                    "后台检查已暂停，冷却结束自动恢复"
                )
            lines.append(
                "请求节流间隔：当前 "
                f"{self._client.current_min_gap():.1f} 秒"
                f"（基础 {self._client.base_min_gap():.1f} 秒，"
                "遭遇风控时会自动拉长）"
            )

        if not pil_available():
            lines.append("")
            lines.append("注意：未检测到 Pillow，动态/专栏无法合成图片。")
        if not _NODE_AVAILABLE:
            lines.append("")
            lines.append("注意：当前 AstrBot 不支持合并转发，将只发送合成图。")

        yield event.plain_result("\n".join(lines))
        event.stop_event()

    # ------------------------------------------------------------------
    # 命令：订阅干跑
    # ------------------------------------------------------------------
    @filter.command("订阅干跑")
    async def cmd_dry_run(self, event: AstrMessageEvent):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return
        if self._pusher is None:
            yield event.plain_result("插件未初始化")
            event.stop_event()
            return

        self._refresh_config_and_subs()
        try:
            lines = await self._pusher.dry_run()
        except Exception as exc:
            logger.exception("bili-subscription 干跑失败")
            lines = [f"干跑失败：{exc}"]

        yield event.plain_result(
            "\n".join(lines) if lines else "当前没有可干跑的订阅。"
        )
        event.stop_event()

    # ------------------------------------------------------------------
    # 命令：订阅检查
    # ------------------------------------------------------------------
    @filter.command("订阅检查")
    async def cmd_force_check(self, event: AstrMessageEvent):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return
        if self._pusher is None:
            yield event.plain_result("插件未初始化")
            event.stop_event()
            return

        self._refresh_config_and_subs()
        try:
            lines = await self._pusher.force_check()
        except Exception as exc:
            logger.exception("bili-subscription 强制检查失败")
            lines = [f"强制检查失败：{exc}"]

        yield event.plain_result("\n".join(lines))
        event.stop_event()

    # ------------------------------------------------------------------
    # 命令：订阅测试
    # ------------------------------------------------------------------
    @filter.command("订阅测试")
    async def cmd_test_push(self, event: AstrMessageEvent, arg: str = ""):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return
        if self._pusher is None or self._client is None:
            yield event.plain_result("插件未初始化")
            event.stop_event()
            return

        self._refresh_config_and_subs()
        uid = (arg or "").strip()
        if not uid:
            subs, _ = self._parse_subs()
            if not subs:
                yield event.plain_result("当前没有订阅，无法测试。")
                event.stop_event()
                return
            uid = subs[0].uid

        lines = [f"UID {uid} 最新内容测试："]
        try:
            lines.extend(
                await self._pusher.push_latest_dynamic_and_article(
                    uid, event.unified_msg_origin
                )
            )
        except Exception as exc:
            logger.exception("bili-subscription 测试推送失败")
            lines.append(f"推送异常：{exc}")

        # 顺带测试视频推送（下载并发送最新视频）
        try:
            lines.extend(
                await self._pusher.push_latest_video(
                    uid, event.unified_msg_origin
                )
            )
        except Exception as exc:
            logger.exception("bili-subscription 测试推送视频失败")
            lines.append(f"视频推送异常：{exc}")

        yield event.plain_result("\n".join(lines))
        event.stop_event()

    # ------------------------------------------------------------------
    # 命令：订阅诊断
    # ------------------------------------------------------------------
    @filter.command("订阅诊断")
    async def cmd_diagnose(self, event: AstrMessageEvent, arg: str = ""):
        if not self._is_admin(event):
            yield event.plain_result(self._deny_reply())
            event.stop_event()
            return
        if self._client is None or self._store is None:
            yield event.plain_result("插件未初始化")
            event.stop_event()
            return

        self._refresh_config_and_subs()
        uid = (arg or "").strip()
        subs, _ = self._parse_subs()
        if not uid:
            if not subs:
                yield event.plain_result("当前没有订阅，无法诊断。")
                event.stop_event()
                return
            uid = subs[0].uid

        target_sub = next((s for s in subs if s.uid == uid), None)

        lines = [f"诊断 UID {uid}"]
        lines.append(f"平台实例前缀（自动检测）：{self._detect_default_adapter()}")
        if target_sub is None:
            lines.append("⚠️ 该 UID 不在订阅列表中")
        else:
            lines.append(f"订阅中的 sessions：{list(target_sub.sessions)}")
            lines.append(f"订阅中的类型：{sorted(target_sub.types)}")

        try:
            videos = await self._client.fetch_videos(uid, limit=5)
        except Exception as exc:
            lines.append(f"视频抓取异常：{exc}")
            videos = []
        lines.append(f"视频候选：{len(videos)} 条")
        for v in videos[:3]:
            lines.append(f"  {v.bvid}  [{v.uploader or '?'}]  {v.title[:40]}")
        stored = await self._store.get(uid, "video")
        lines.append(f"状态文件记录的 video：{stored or '（无）'}")

        try:
            dynamics = await self._client.fetch_dynamics(uid, limit=3)
        except Exception as exc:
            lines.append(f"动态抓取异常：{exc}")
            dynamics = []
        lines.append(f"动态候选：{len(dynamics)} 条")
        for d in dynamics[:3]:
            flags = []
            if d.is_original_video:
                flags.append("视频动态")
            if is_empty_dynamic(d):
                flags.append("空动态")
            flag_text = ("  " + "/".join(flags)) if flags else ""
            lines.append(
                f"  {d.dynamic_id}  kind={d.kind or '?'}{flag_text}  "
                f"图 {len(d.image_urls)} 张  文 {d.text[:30]}"
            )
        stored_dyn = await self._store.get(uid, "dynamic")
        lines.append(f"状态文件记录的 dynamic：{stored_dyn or '（无）'}")

        try:
            articles = await self._client.fetch_articles(uid, limit=3)
        except Exception as exc:
            lines.append(f"专栏抓取异常：{exc}")
            articles = []
        lines.append(f"专栏候选：{len(articles)} 条")
        for a in articles[:3]:
            lines.append(f"  {a.article_id}  {a.title[:40]}")
        stored_art = await self._store.get(uid, "article")
        lines.append(f"状态文件记录的 article：{stored_art or '（无）'}")

        yield event.plain_result("\n".join(lines))
        event.stop_event()


# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------
def _event_str(event: AstrMessageEvent, method_name: str) -> str | None:
    method = getattr(event, method_name, None)
    if not callable(method):
        return None
    try:
        value = method()
    except Exception:
        return None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# 合并转发里单张原图超过该字节数才压缩（否则直接发原图，保证文字可读）
_FORWARD_IMAGE_MAX_BYTES = 6 * 1024 * 1024


def _compress_image(data: bytes, *, max_side: int = 1920, quality: int = 85) -> bytes:
    """压缩超大图片，避免 base64 后过大导致合并转发发送失败。

    只在图片超过 _FORWARD_IMAGE_MAX_BYTES 时才调用，属于兜底，
    因此压缩参数更宽松（1920 边长 / 85 质量），尽量保住文字可读性。
    """
    try:
        from PIL import Image
        _lanczos = getattr(Image, "Resampling", Image).LANCZOS
    except ImportError:
        return data
    try:
        img = Image.open(io.BytesIO(data))
        img = img.convert("RGB")
        w, h = img.size
        longest = max(w, h)
        if longest > max_side:
            ratio = max_side / longest
            new_w = int(w * ratio)
            new_h = int(h * ratio)
            img = img.resize((new_w, new_h), _lanczos)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        result = buf.getvalue()
        return result if len(result) < len(data) else data
    except Exception:
        return data


def _image_from_bytes(data: bytes):
    b64 = base64.b64encode(data).decode("ascii")
    try:
        return Image(file=f"base64://{b64}")
    except TypeError:
        pass
    try:
        return Image.fromBytes(data)
    except Exception:
        return Image.fromBase64(b64)


def _image_from_bytes_compressed(data: bytes):
    """和 _image_from_bytes 一样，但先压缩再编码，用于合并转发的节点。"""
    return _image_from_bytes(_compress_image(data))


class _SplitChain:
    """包装两个 MessageChain：先发合成图，再发合并转发。"""

    def __init__(
        self, *, first: MessageChain | None, forward: MessageChain | None
    ) -> None:
        self.first = first
        self.forward = forward