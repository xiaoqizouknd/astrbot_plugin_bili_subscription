"""AstrBot B 站订阅推送插件入口。"""

from __future__ import annotations

import base64
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

from .bili_api import BiliSubscriptionClient, UserVideo, _USER_AGENT
from .bili_player_bridge import (
    BiliVideoDownloader,
    has_bilibili_login,
    read_bilibili_cookies,
)
from .pusher import SubscriptionPusher, is_empty_dynamic
from .render import pil_available
from .subscription import (
    Subscription,
    SubscriptionStateStore,
    parse_subscriptions,
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


async def _warmup_bilibili(
    session: aiohttp.ClientSession, cache_path: Path
) -> None:
    """预访问 B 站首页，让 CookieJar 拿到 buvid3 / b_nut 等基础 Cookie。

    - 先尝试从 cache_path 恢复上次的 buvid3，让"设备身份"连续；
    - 再访问一次首页，拿到或刷新 cookie；
    - 把 buvid3 / b_nut / _uuid 缓存到 cache_path。

    这些 Cookie 无需登录即可获得，拿到后能显著降低被 412 的概率。
    """
    target_url = URL("https://www.bilibili.com/")

    # 1) 尝试恢复历史 cookie
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

    # 2) 预访问首页
    try:
        async with session.get(
            "https://www.bilibili.com/",
            headers={"User-Agent": _USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            await resp.read()
    except Exception as exc:
        logger.warning("bili-subscription 预访问首页失败（不影响运行）：%s", exc)
        return

    # 3) 提取并保存 buvid 相关 cookie
    try:
        jar = session.cookie_jar.filter_cookies(target_url)
        fresh = {
            name: morsel.value
            for name, morsel in jar.items()
            if name.startswith("buvid") or name in ("b_nut", "_uuid")
        }
        if fresh:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps(fresh, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(
                "bili-subscription 已获取设备标识：%s",
                ", ".join(fresh.keys()),
            )
        else:
            logger.warning(
                "bili-subscription 预访问首页未拿到 buvid，"
                "可能是网络问题或被风控，稍后会自动重试"
            )
    except Exception as exc:
        logger.debug("bili-subscription 保存 buvid 缓存失败：%s", exc)


class BiliSubscriptionPlugin(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None) -> None:
        super().__init__(context, config)
        self._config = dict(config or {})
        self._http: aiohttp.ClientSession | None = None
        self._client: BiliSubscriptionClient | None = None
        self._store: SubscriptionStateStore | None = None
        self._pusher: SubscriptionPusher | None = None
        self._downloader: BiliVideoDownloader | None = None
        self._bili_player_data_dir: Path | None = None
        self._initialized = False
        self._last_subs_signature: tuple[str, ...] = ()

    # ------------------------------------------------------------------
    # 配置
    # ------------------------------------------------------------------
    def _refresh_config_from_astrbot(self) -> None:
        """从 AstrBot 重新读取插件配置。

        不同 AstrBot 版本的配置结构不同，因此依次尝试几个可能的路径；
        命中任何一个就更新本地配置并返回。

        注意：绝不把整个应用配置合并进来，否则会污染插件配置。
        """
        candidates: list[object] = []

        # Star 基类的 config 属性通常就是插件作用域配置，最可靠
        star_cfg = getattr(self, "config", None)
        if isinstance(star_cfg, dict):
            nested = star_cfg.get(PLUGIN_NAME)
            if isinstance(nested, dict):
                candidates.append(nested)
            candidates.append(star_cfg)

        # AstrBot 全局配置里的插件命名空间
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

        # 除订阅列表之外的开关也要热重载
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
                    max_items_per_push=int(
                        self._config.get("max_items_per_push") or 5
                    ),
                    scan_interval_seconds=int(
                        self._config.get("scan_interval_seconds") or 60
                    ),
                    font_path=str(
                        self._config.get("image_font_path") or ""
                    ) or None,
                )
            except Exception:
                logger.exception("bili-subscription 刷新开关失败")

        subs, errors = self._parse_subs()
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

    def _parse_subs(self) -> tuple[list[Subscription], list[str]]:
        return parse_subscriptions(
            self._config.get("subscriptions"),
            default_adapter=self._detect_default_adapter(),
            default_types=str(
                self._config.get("default_types") or "video,dynamic,article"
            ).strip() or "video,dynamic,article",
            default_interval_minutes=int(
                self._config.get("default_interval_minutes") or 15
            ),
        )

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
            # 预访问 B 站首页，拿到 buvid3 等基础 Cookie
            await _warmup_bilibili(http, data_dir / "buvid_cache.json")

            client = BiliSubscriptionClient(
                http,
                credentials_getter=lambda: read_bilibili_cookies(bili_player_dir),
                min_request_gap=float(
                    self._config.get("min_request_gap_seconds") or 1.5
                ),
            )
            store = SubscriptionStateStore(data_dir / "subscription_state.json")
            await store.load()

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
                config_refresher=self._refresh_config_and_subs,
                max_concurrent_checks=int(
                    self._config.get("max_concurrent_checks") or 2
                ),
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
                    await self.context.send_message(session_id, chain.first)
                    sent = True
                if chain.forward is not None:
                    await self.context.send_message(session_id, chain.forward)
                    sent = True
                return sent
            await self.context.send_message(session_id, chain)
            return True
        except Exception:
            logger.exception("bili-subscription 向 %s 推送失败", session_id)
            return False

    def _build_dynamic_chain(
        self, card_bytes: bytes | None, images: list[bytes]
    ) -> Any:
        first_chain: MessageChain | None = None
        if card_bytes:
            first_chain = MessageChain([_image_from_bytes(card_bytes)])

        forward_chain: MessageChain | None = None
        if (
            bool(self._config.get("enable_node_forward", True))
            and _NODE_AVAILABLE
            and Node is not None and Nodes is not None
            and images
        ):
            nodes: list[Any] = []
            for index, img in enumerate(images, start=1):
                nodes.append(
                    Node(
                        content=[Plain(f"原图 {index}"), _image_from_bytes(img)],
                        name="B站更新", uin="10000",
                    )
                )
            if card_bytes:
                nodes.append(
                    Node(
                        content=[Plain("合并卡片"), _image_from_bytes(card_bytes)],
                        name="B站更新", uin="10000",
                    )
                )
            forward_chain = MessageChain([Nodes(nodes)])

        return _SplitChain(first=first_chain, forward=forward_chain)

    async def _send_video_to_sessions(
        self, sessions: tuple[str, ...], video: UserVideo
    ) -> bool:
        """下载一次视频，向所有会话发送。返回是否全部成功。"""
        if self._downloader is None or not self._downloader.ffmpeg_available:
            logger.warning("bili-subscription 视频下载器不可用，跳过 %s", video.bvid)
            return False

        downloaded = await self._downloader.download(video.bvid)
        if downloaded is None:
            logger.warning("bili-subscription 视频下载失败：%s", video.bvid)
            return False

        try:
            all_ok = True
            for session_id in sessions:
                try:
                    component = Video.fromFileSystem(str(downloaded.path))
                    chain = MessageChain(
                        [
                            Plain(
                                f"【视频更新】{video.title}\n"
                                f"https://www.bilibili.com/video/{video.bvid}"
                            ),
                            component,
                        ]
                    )
                    await self.context.send_message(session_id, chain)
                except Exception:
                    logger.exception(
                        "bili-subscription 发送视频 %s 到 %s 失败",
                        video.bvid, session_id,
                    )
                    all_ok = False
            return all_ok
        finally:
            try:
                await downloaded.release()
            except Exception:
                logger.exception("bili-subscription 释放视频文件失败")

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
        lines.append(
            "视频动态过滤："
            f"{'已开启' if self._config.get('skip_video_dynamic', True) else '已关闭'}"
        )
        lines.append(
            "空动态过滤："
            f"{'已开启' if self._config.get('skip_empty_dynamic', True) else '已关闭'}"
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
            lines.append(f"  {v.bvid}  {v.title[:40]}")
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


class _SplitChain:
    """包装两个 MessageChain：先发合成图，再发合并转发。"""

    def __init__(
        self, *, first: MessageChain | None, forward: MessageChain | None
    ) -> None:
        self.first = first
        self.forward = forward