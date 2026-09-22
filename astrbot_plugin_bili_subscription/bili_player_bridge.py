"""从原插件 astrbot_plugin_bili_player 读取登录 Cookie，以及自包含的视频下载器。

只做文件读取，不 import 原插件，避免 sys.path 和版本兼容问题。

下载预算机制：
- 调用方通过 max_total_bytes 传入"下载总预算"（来自配置 max_video_send_mb）；
- 预算采用**动态分配**：先给视频轨一个保守上限，下完后根据实际大小
  决定音频轨的剩余预算，而不是下载前静态切分；
- 通过 HTTP Content-Length 在下载开始前就判断，避免下完/合并完才发现超限；
  但 B 站部分 CDN 走 chunked 编码不带 Content-Length，
  此时靠下载过程中的流式检查兜底——两道防线合起来才是完整的"提前放弃"；
- max_total_bytes <= 0 表示"不限制"，用内置硬上限兜底，防止磁盘被打满。
"""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from astrbot.api import logger

from .bili_api import BiliSubscriptionClient


def read_bilibili_cookies(bili_player_data_dir: Path) -> dict[str, str]:
    """读取原插件 accounts.json 中的登录 Cookie；只读，不修改。"""
    accounts_path = Path(bili_player_data_dir) / "accounts.json"
    if not accounts_path.exists():
        return {}
    try:
        payload = json.loads(accounts_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("bili-subscription 读取 accounts.json 失败：%s", exc)
        return {}
    if not isinstance(payload, dict):
        return {}
    record = payload.get("bilibili")
    if not isinstance(record, dict):
        return {}
    cookies = record.get("cookies")
    if not isinstance(cookies, dict):
        return {}
    return {
        str(name): str(value)
        for name, value in cookies.items()
        if isinstance(name, str)
        and isinstance(value, str)
        and value
        and "\r" not in value
        and "\n" not in value
    }


def has_bilibili_login(bili_player_data_dir: Path) -> bool:
    return bool(read_bilibili_cookies(bili_player_data_dir))


@dataclass(slots=True)
class DownloadedVideo:
    """下载完成的视频。

    page_count / page_title 用于向用户提示"这是多 P 视频，仅推送第 1 P"；
    title / uploader 是下载时拿到的权威标题与 UP 主名（比列表接口更完整）。
    _release 为 None 时 release() 是空操作（构造时未传入释放函数）。
    """

    path: Path
    page_count: int = 1
    page_title: str = ""
    title: str = ""
    uploader: str = ""
    _release: Callable[[], Awaitable[None]] | None = None

    async def release(self) -> None:
        if self._release is None:
            return
        await self._release()


class BiliVideoDownloader:
    """自包含的视频下载器：解析、下载、ffmpeg 合并。

    预算模型（动态分配）：
    ┌──────────────────────────────────────────────────┐
    │ 1. 视频轨预算                                     │
    │    DASH 流：max_total − AUDIO_MIN_BYTES           │
    │    （给音频预留最小可用空间，不浪费预算）          │
    │    progressive 流：max_total                      │
    │    max_total ≤ 0：用内置硬上限                    │
    │                                                   │
    │ 2. 下载视频轨，得到实际大小 actual_video_size      │
    │                                                   │
    │ 3. 音频轨预算                                     │
    │    min(AUDIO_MAX_BYTES, max_total − actual_video) │
    │    小于 AUDIO_MIN_BYTES 时放弃（视为预算耗尽）      │
    │                                                   │
    │ 4. max_total ≤ 0 时：音频直接用 AUDIO_MAX_BYTES    │
    └──────────────────────────────────────────────────┘

    与配置滑杆 max_video_send_mb（默认上限 500）对齐：
        DASH：视频轨 ≤ 499MB（预留 1MB），音频 ≤ 30MB → 总 ≤ 500MB
        实际音频一般 3~10MB，视频轨能用到 ~490MB，比静态切分更充分。

    与配置滑杆 max_video_send_mb（上限 500）配合：
        用户填 500 → 单文件理论最大 500MB，两层上限完全对齐。
    """

    # 音频轨绝对上限；超过这个尺寸视为异常音频流，直接放弃
    AUDIO_MAX_BYTES = 30 * 1024 * 1024
    # 给音频预留的最小预算（约 1MB，够 128kbps × 60s 的音频）
    AUDIO_MIN_BYTES = 1 * 1024 * 1024
    # 无预算时的视频轨硬上限（与滑杆 max=500 时的视频轨上限对齐）
    DEFAULT_VIDEO_HARD_CAP = 470 * 1024 * 1024
    # 无预算时的 progressive 硬上限（视频+音频合一，等于滑杆上限）
    DEFAULT_PROGRESSIVE_HARD_CAP = 500 * 1024 * 1024

    DOWNLOAD_TIMEOUT_SECONDS = 900.0
    MERGE_TIMEOUT_SECONDS = 300.0
    # 超过该时长的残留媒体文件在启动时清理
    STALE_MEDIA_MAX_AGE_SECONDS = 2 * 86400

    def __init__(
        self,
        *,
        client: BiliSubscriptionClient,
        data_dir: Path,
        ffmpeg_binary: str = "ffmpeg",
    ) -> None:
        self._client = client
        self._data_dir = Path(data_dir)
        self._media_dir = self._data_dir / "media"
        self._ffmpeg = shutil.which(ffmpeg_binary)
        # 每个视频（bvid-cid）一把锁，防止手动检查和后台轮询同时下载同一文件
        self._download_locks: dict[str, asyncio.Lock] = {}
        self._cleanup_media_dir()

    def _cleanup_media_dir(self) -> None:
        """启动时清理崩溃残留的临时文件与过期媒体，避免磁盘越积越多。"""
        if not self._media_dir.exists():
            return
        cutoff = time.time() - self.STALE_MEDIA_MAX_AGE_SECONDS
        removed = 0
        try:
            for pattern in ("*.part", "*.m4s", "*.mp4"):
                for path in self._media_dir.glob(pattern):
                    try:
                        # .part 无条件清理；成品文件只清超过保留时长的
                        if pattern == "*.part" or path.stat().st_mtime < cutoff:
                            path.unlink()
                            removed += 1
                    except OSError:
                        pass
        except Exception:
            return
        if removed:
            logger.info(
                "bili-subscription 已清理 %d 个残留/过期媒体文件", removed
            )

    def _lock_for(self, token: str) -> asyncio.Lock:
        lock = self._download_locks.get(token)
        if lock is None:
            lock = asyncio.Lock()
            self._download_locks[token] = lock
            # 容量保护：只保留正在使用的锁
            if len(self._download_locks) > 200:
                self._download_locks = {
                    key: value
                    for key, value in self._download_locks.items()
                    if value.locked()
                }
        return lock

    @property
    def ffmpeg_available(self) -> bool:
        return bool(self._ffmpeg)

    # ------------------------------------------------------------------
    # 预算计算
    # ------------------------------------------------------------------
    def _video_budget(self, max_total: int, *, dash: bool) -> int:
        """视频轨下载预算。

        dash=True：后面还要下独立音频轨，给音频预留 AUDIO_MIN_BYTES。
        dash=False：progressive 流（视频+音频合一），全部预算给视频轨。
        max_total <= 0：用内置硬上限。
        """
        if max_total <= 0:
            return (
                self.DEFAULT_VIDEO_HARD_CAP
                if dash
                else self.DEFAULT_PROGRESSIVE_HARD_CAP
            )
        if dash:
            # 给音频预留最小预算，其余全部给视频轨
            return max(1, max_total - self.AUDIO_MIN_BYTES)
        return max(1, max_total)

    def _audio_budget(self, max_total: int, video_size: int) -> int:
        """根据视频轨实际大小，动态计算音频轨预算。

        返回值 <= 0 表示预算已耗尽，音频放弃下载（整个视频降级）。
        """
        if max_total <= 0:
            return self.AUDIO_MAX_BYTES
        remaining = max_total - video_size
        if remaining < self.AUDIO_MIN_BYTES:
            return 0
        return min(self.AUDIO_MAX_BYTES, remaining)

    # ------------------------------------------------------------------
    # 下载
    # ------------------------------------------------------------------
    async def download(
        self,
        bvid: str,
        *,
        max_total_bytes: int = 0,
    ) -> DownloadedVideo | None:
        """下载视频。

        max_total_bytes：
        - > 0：下载总预算（视频轨 + 音频轨），来自配置；
        - <= 0：不限制，用内置硬上限兜底。
        """
        if not self._ffmpeg:
            return None

        detail = await self._client.get_video_detail(bvid)
        if not detail.pages:
            return None
        page = detail.pages[0]
        page_count = len(detail.pages)
        page_title = page.title

        stream = await self._client.resolve_video_stream(detail.bvid, page.cid)
        if stream is None:
            return None

        token = f"{detail.bvid}-{page.cid}"

        # 同一视频的并发下载互斥：手动"订阅检查"与后台轮询可能同时命中
        # 同一个新视频，避免两个任务写同一个 .part 文件互相破坏。
        async with self._lock_for(token):
            self._media_dir.mkdir(parents=True, exist_ok=True)

            video_path = self._media_dir / f"{token}.video.m4s"
            audio_path = self._media_dir / f"{token}.audio.m4s"
            output_path = self._media_dir / f"{token}.mp4"

            # 视频轨预算：DASH 流给音频留最小预留
            video_budget = self._video_budget(
                max_total_bytes, dash=stream.needs_remux
            )

            try:
                ok = await self._client.download_to_file(
                    stream.video, video_path,
                    headers=stream.headers,
                    max_bytes=video_budget,
                    timeout_seconds=self.DOWNLOAD_TIMEOUT_SECONDS,
                )
                if not ok:
                    return None

                if stream.needs_remux:
                    if stream.audio is None:
                        return None

                    # 下完视频轨后，才能知道实际大小，进而算音频预算
                    try:
                        actual_video_size = video_path.stat().st_size
                    except OSError:
                        return None

                    audio_budget = self._audio_budget(
                        max_total_bytes, actual_video_size
                    )
                    if audio_budget <= 0:
                        logger.warning(
                            "bili-subscription 视频轨 %.1fMB 已耗尽预算，"
                            "放弃音频下载：%s",
                            actual_video_size / 1024 / 1024, bvid,
                        )
                        return None

                    ok = await self._client.download_to_file(
                        stream.audio, audio_path,
                        headers=stream.headers,
                        max_bytes=audio_budget,
                        timeout_seconds=self.DOWNLOAD_TIMEOUT_SECONDS,
                    )
                    if not ok:
                        return None
                    if not await self._merge(video_path, audio_path, output_path):
                        return None
                else:
                    video_path.replace(output_path)
            except Exception:
                video_path.unlink(missing_ok=True)
                audio_path.unlink(missing_ok=True)
                output_path.unlink(missing_ok=True)
                return None
            finally:
                video_path.unlink(missing_ok=True)
                audio_path.unlink(missing_ok=True)

            if not output_path.is_file() or output_path.stat().st_size == 0:
                output_path.unlink(missing_ok=True)
                return None

            async def _release() -> None:
                try:
                    output_path.unlink(missing_ok=True)
                except OSError:
                    pass

            return DownloadedVideo(
                path=output_path,
                page_count=page_count,
                page_title=page_title,
                title=detail.title,
                uploader=detail.uploader,
                _release=_release,
            )

    async def _merge(self, video: Path, audio: Path, output: Path) -> bool:
        if not self._ffmpeg:
            return False
        output.unlink(missing_ok=True)
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                self._ffmpeg,
                "-hide_banner", "-loglevel", "error", "-y",
                "-i", str(video), "-i", str(audio),
                "-c:v", "copy", "-c:a", "copy",
                "-movflags", "+faststart",
                str(output),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                await asyncio.wait_for(
                    process.communicate(), timeout=self.MERGE_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "bili-subscription ffmpeg 合并超时，已终止：%s", output.name
                )
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                try:
                    await process.communicate()
                except Exception:
                    pass
                output.unlink(missing_ok=True)
                return False
        except Exception:
            output.unlink(missing_ok=True)
            return False
        if process.returncode != 0 or not output.is_file() or output.stat().st_size == 0:
            output.unlink(missing_ok=True)
            return False
        return True

    async def aclose(self) -> None:
        return None