"""从原插件 astrbot_plugin_bili_player 读取登录 Cookie，以及自包含的视频下载器。

只做文件读取，不 import 原插件，避免 sys.path 和版本兼容问题。
"""

from __future__ import annotations

import asyncio
import json
import shutil
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
    path: Path
    _release: Callable[[], Awaitable[None]]

    async def release(self) -> None:
        await self._release()


class BiliVideoDownloader:
    """自包含的视频下载器：解析、下载、ffmpeg 合并。"""

    MAX_VIDEO_BYTES = 150 * 1024 * 1024
    MAX_AUDIO_BYTES = 30 * 1024 * 1024
    DOWNLOAD_TIMEOUT_SECONDS = 900.0

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

    @property
    def ffmpeg_available(self) -> bool:
        return bool(self._ffmpeg)

    async def download(self, bvid: str) -> DownloadedVideo | None:
        if not self._ffmpeg:
            return None

        detail = await self._client.get_video_detail(bvid)
        if not detail.pages:
            return None
        page = detail.pages[0]

        stream = await self._client.resolve_video_stream(detail.bvid, page.cid)
        if stream is None:
            return None

        token = f"{detail.bvid}-{page.cid}"
        self._media_dir.mkdir(parents=True, exist_ok=True)

        video_path = self._media_dir / f"{token}.video.m4s"
        audio_path = self._media_dir / f"{token}.audio.m4s"
        output_path = self._media_dir / f"{token}.mp4"

        try:
            ok = await self._client.download_to_file(
                stream.video, video_path,
                headers=stream.headers,
                max_bytes=self.MAX_VIDEO_BYTES,
                timeout_seconds=self.DOWNLOAD_TIMEOUT_SECONDS,
            )
            if not ok:
                return None

            if stream.needs_remux:
                if stream.audio is None:
                    return None
                ok = await self._client.download_to_file(
                    stream.audio, audio_path,
                    headers=stream.headers,
                    max_bytes=self.MAX_AUDIO_BYTES,
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

        return DownloadedVideo(path=output_path, _release=_release)

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
            await process.communicate()
        except Exception:
            output.unlink(missing_ok=True)
            return False
        if process.returncode != 0 or not output.is_file() or output.stat().st_size == 0:
            output.unlink(missing_ok=True)
            return False
        return True

    async def aclose(self) -> None:
        return None