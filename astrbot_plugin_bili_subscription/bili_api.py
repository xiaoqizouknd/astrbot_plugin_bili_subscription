"""B 站 API 客户端：订阅抓取、视频解析、媒体下载。

防风控策略：
- 全局请求节流（两次请求之间至少间隔 N 秒）；
- 请求头补全（Origin / Accept-Language）；
- Cookie 30 秒缓存，避免高频读文件；
- 遇到限流错误码（-352 / -412 / 429 等）自动指数退避重试；
- 提供 check_login() 供上层定期检测登录态。

接口失败抛 ``BiliApiError``（携带 B 站业务码），由调度层决定如何处理。
图片下载失败只写 debug 日志，不影响主流程。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp

from astrbot.api import logger


API_ORIGIN = "https://api.bilibili.com"
_NAV_URL = f"{API_ORIGIN}/x/web-interface/nav"
_DYNAMIC_URL = f"{API_ORIGIN}/x/polymer/web-dynamic/v1/feed/space"
_VIDEO_LIST_URL = f"{API_ORIGIN}/x/space/wbi/arc/search"
_ARTICLE_URL = f"{API_ORIGIN}/x/space/wbi/article"
_VIEW_URL = f"{API_ORIGIN}/x/web-interface/wbi/view"
_PLAY_URL = f"{API_ORIGIN}/x/player/wbi/playurl"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_WBI_TTL = 600
_DASH_VIDEO_QUALITY = 32
_IMAGE_MAX_BYTES = 8 * 1024 * 1024

_MIXIN_INDICES = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 62, 6, 63, 57, 20, 34, 52, 59, 11, 36, 44,
)
_FILTER = str.maketrans("", "", "!'()*")

# 表示"暂时被限流"的错误码，适合退避后重试。
# B 站业务码 + HTTP 状态码都要覆盖。
_RETRYABLE_CODES = frozenset({
    -352, -412, -509, -799,                # B 站业务码：风控 / 拦截 / 限流
    408, 412, 429, 500, 502, 503, 504,     # HTTP 状态码
})

# 退避等待序列，单位秒
_BACKOFF_DELAYS = (5.0, 15.0, 60.0)

CredentialsGetter = Callable[[], Mapping[str, str]]


class BiliApiError(RuntimeError):
    """B 站接口错误。``code`` 是 B 站业务码，-1 表示本地错误。"""

    def __init__(self, message: str, *, code: int = -1) -> None:
        super().__init__(message)
        self.code = code


# ----------------------------------------------------------------------
# 数据模型
# ----------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class UserVideo:
    bvid: str
    title: str
    cover: str
    duration_seconds: int
    created_at: int


@dataclass(frozen=True, slots=True)
class UserArticle:
    article_id: str
    title: str
    summary: str
    covers: tuple[str, ...]
    created_at: int


@dataclass(frozen=True, slots=True)
class UserDynamic:
    dynamic_id: str
    text: str
    image_urls: tuple[str, ...]
    created_at: int
    kind: str
    is_original_video: bool = False


@dataclass(frozen=True, slots=True)
class VideoPage:
    cid: int
    index: int
    title: str
    duration_ms: int


@dataclass(frozen=True, slots=True)
class VideoDetail:
    bvid: str
    title: str
    uploader: str
    pages: tuple[VideoPage, ...]


@dataclass(frozen=True, slots=True)
class DashStream:
    url: str
    backup_urls: tuple[str, ...]
    mime_type: str
    codecs: str = ""


@dataclass(frozen=True, slots=True)
class ResolvedVideoStream:
    video: DashStream
    audio: DashStream | None
    headers: dict[str, str]
    needs_remux: bool
    duration_ms: int | None


# ----------------------------------------------------------------------
# WBI 签名
# ----------------------------------------------------------------------
def _derive_mixin_key(img_url: str, sub_url: str) -> str:
    img_key = img_url.rsplit("/", 1)[-1].split(".", 1)[0]
    sub_key = sub_url.rsplit("/", 1)[-1].split(".", 1)[0]
    raw = img_key + sub_key
    mixed = "".join(raw[i] for i in _MIXIN_INDICES if i < len(raw))
    if len(mixed) < 32:
        raise BiliApiError("B 站返回了无效的 WBI 密钥")
    return mixed[:32]


def _sign_params(
    params: Mapping[str, Any], mixin_key: str, *, timestamp: int | None = None
) -> dict[str, str]:
    canonical = {
        str(k): str(v).translate(_FILTER)
        for k, v in params.items()
        if v is not None
    }
    canonical["wts"] = str(int(time.time()) if timestamp is None else timestamp)
    ordered = dict(sorted(canonical.items()))
    query = urlencode(tuple(ordered.items()), quote_via=quote, safe="")
    ordered["w_rid"] = hashlib.md5(f"{query}{mixin_key}".encode("utf-8")).hexdigest()
    return ordered


# ----------------------------------------------------------------------
# 客户端
# ----------------------------------------------------------------------
class BiliSubscriptionClient:
    """极简 B 站 API 客户端。失败时抛 ``BiliApiError``。"""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        credentials_getter: CredentialsGetter | None = None,
        *,
        min_request_gap: float = 1.5,
    ) -> None:
        self._session = session
        self._credentials_getter = credentials_getter
        self._wbi_key: str | None = None
        self._wbi_expires = 0.0
        self._wbi_lock = asyncio.Lock()

        # 全局请求节流：两次请求之间至少间隔 _min_gap 秒
        self._min_gap = max(0.0, float(min_request_gap))
        self._last_request_ts = 0.0
        self._request_lock = asyncio.Lock()

        # Cookie 缓存，避免高频读文件
        self._cred_cache: dict[str, str] = {}
        self._cred_expires = 0.0

    # ------------------------------------------------------------------
    # 订阅抓取
    # ------------------------------------------------------------------
    async def fetch_videos(self, uid: str, *, limit: int = 5) -> list[UserVideo]:
        data = await self._request_with_backoff(
            _VIDEO_LIST_URL,
            {"mid": uid, "ps": limit, "pn": 1, "order": "pubdate"},
            wbi=True,
        )
        vlist = data.get("list", {}).get("vlist") if isinstance(data, dict) else None
        if not isinstance(vlist, list):
            return []
        result: list[UserVideo] = []
        for raw in vlist[:limit]:
            if not isinstance(raw, Mapping):
                continue
            bvid = str(raw.get("bvid") or "").strip()
            if not bvid:
                continue
            result.append(
                UserVideo(
                    bvid=bvid,
                    title=str(raw.get("title") or "").strip(),
                    cover=str(raw.get("pic") or "").strip(),
                    duration_seconds=_parse_duration(str(raw.get("length") or "")),
                    created_at=int(raw.get("created") or 0),
                )
            )
        return result

    async def fetch_articles(self, uid: str, *, limit: int = 5) -> list[UserArticle]:
        data = await self._request_with_backoff(
            _ARTICLE_URL,
            {"mid": uid, "ps": limit, "pn": 1, "sort": "publish_time"},
            wbi=True,
        )
        articles = data.get("articles") if isinstance(data, dict) else None
        if not isinstance(articles, list):
            return []
        result: list[UserArticle] = []
        for raw in articles[:limit]:
            if not isinstance(raw, Mapping):
                continue
            article_id = str(raw.get("id") or raw.get("cvid") or "").strip()
            if not article_id:
                continue
            covers = raw.get("image_urls") or []
            cover_list = tuple(str(c) for c in covers if isinstance(c, str))
            result.append(
                UserArticle(
                    article_id=article_id,
                    title=str(raw.get("title") or "").strip(),
                    summary=str(raw.get("summary") or "").strip(),
                    covers=cover_list[:3],
                    created_at=int(raw.get("publish_time") or 0),
                )
            )
        return result

    async def fetch_dynamics(self, uid: str, *, limit: int = 5) -> list[UserDynamic]:
        # 动态接口不需要 WBI 签名（B 站当前行为），不要擅自改成 True。
        data = await self._request_with_backoff(
            _DYNAMIC_URL,
            {"host_mid": uid, "timezone_offset": -480, "offset": ""},
            wbi=False,
        )
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        result: list[UserDynamic] = []
        for raw in items[:limit]:
            parsed = _parse_dynamic(raw)
            if parsed is not None:
                result.append(parsed)
        return result

    # ------------------------------------------------------------------
    # 登录态检测
    # ------------------------------------------------------------------
    async def check_login(self) -> bool:
        """轻量检测当前 Cookie 是否仍有效。不抛异常。"""
        try:
            data = await self._request(_NAV_URL, {}, wbi=False)
        except BiliApiError:
            return False
        is_login = bool(data.get("isLogin"))
        if not is_login:
            logger.warning(
                "bili-subscription B 站登录态已失效，"
                "请在 astrbot_plugin_bili_player 里重新扫码登录。"
            )
        return is_login

    # ------------------------------------------------------------------
    # 视频解析与下载
    # ------------------------------------------------------------------
    async def get_video_detail(self, bvid: str) -> VideoDetail:
        data = await self._request(_VIEW_URL, {"bvid": bvid}, wbi=True)
        if not isinstance(data, Mapping):
            raise BiliApiError(f"视频 {bvid} 返回了未知数据")

        resolved_bvid = str(data.get("bvid") or bvid).strip()
        pages: list[VideoPage] = []
        raw_pages = data.get("pages")
        if isinstance(raw_pages, list):
            for raw_page in raw_pages:
                if not isinstance(raw_page, Mapping):
                    continue
                cid = _to_int(raw_page.get("cid"))
                if cid <= 0:
                    continue
                pages.append(
                    VideoPage(
                        cid=cid,
                        index=max(1, _to_int(raw_page.get("page"), len(pages) + 1)),
                        title=str(raw_page.get("part") or "").strip(),
                        duration_ms=max(0, _to_int(raw_page.get("duration"))) * 1000,
                    )
                )
        if not pages:
            cid = _to_int(data.get("cid"))
            if cid > 0:
                pages.append(
                    VideoPage(
                        cid=cid,
                        index=1,
                        title=str(data.get("title") or "").strip(),
                        duration_ms=max(0, _to_int(data.get("duration"))) * 1000,
                    )
                )
        uploader = ""
        owner = data.get("owner")
        if isinstance(owner, Mapping):
            uploader = str(owner.get("name") or "").strip()
        return VideoDetail(
            bvid=resolved_bvid,
            title=str(data.get("title") or resolved_bvid).strip(),
            uploader=uploader or "未知上传者",
            pages=tuple(pages),
        )

    async def resolve_video_stream(
        self, bvid: str, cid: int
    ) -> ResolvedVideoStream | None:
        data = await self._request(
            _PLAY_URL,
            {
                "bvid": bvid,
                "cid": cid,
                "qn": _DASH_VIDEO_QUALITY,  # 请求 480P；实际取 DASH 最低 id 的 AVC
                "fnval": 16,
                "fnver": 0,
                "fourk": 0,
                "platform": "pc",
            },
            wbi=True,
        )
        if not isinstance(data, Mapping):
            return None
        duration_ms = _to_int(data.get("timelength")) or None
        headers = self._stream_headers(bvid)

        dash = data.get("dash")
        video = _select_dash_video(dash)
        if video is not None:
            return ResolvedVideoStream(
                video=video,
                audio=_select_dash_audio(dash),
                headers=headers,
                needs_remux=True,
                duration_ms=duration_ms,
            )
        progressive = _single_progressive(data.get("durl"))
        if progressive is not None:
            return ResolvedVideoStream(
                video=progressive,
                audio=None,
                headers=headers,
                needs_remux=False,
                duration_ms=duration_ms,
            )
        return None

    async def download_to_file(
        self,
        stream: DashStream,
        dest: Path,
        *,
        headers: Mapping[str, str],
        max_bytes: int,
        timeout_seconds: float,
    ) -> bool:
        for url in (stream.url, *stream.backup_urls):
            if not url:
                continue
            if await self._download_one(
                url, dest,
                headers=headers,
                max_bytes=max_bytes,
                timeout_seconds=timeout_seconds,
            ):
                return True
        return False

    async def download_image(
        self, url: str, *, max_bytes: int = _IMAGE_MAX_BYTES
    ) -> bytes | None:
        """下载单张图片。失败返回 None（不抛异常，图片缺失不影响主流程）。"""
        if not url:
            return None
        headers = self._base_headers(referer="https://www.bilibili.com/")
        try:
            async with self._session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()
                if len(data) > max_bytes:
                    return None
                return data
        except Exception as exc:
            logger.debug("bili-api 图片下载失败 %s：%s", url, exc)
            return None

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------
    async def _download_one(
        self, url: str, dest: Path, *,
        headers: Mapping[str, str],
        max_bytes: int,
        timeout_seconds: float,
    ) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_suffix(dest.suffix + ".part")
        try:
            async with self._session.get(
                url, headers=dict(headers),
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(
                    connect=15, sock_read=120, total=timeout_seconds
                ),
            ) as resp:
                if not (200 <= resp.status < 300):
                    return False
                content_length = _to_int(resp.headers.get("Content-Length"), 0)
                if content_length and content_length > max_bytes:
                    logger.warning(
                        "bili-api 流声明 %d 字节，超过 %d 上限",
                        content_length, max_bytes,
                    )
                    return False
                size = 0
                with part.open("wb") as f:
                    async for chunk in resp.content.iter_chunked(128 * 1024):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > max_bytes:
                            raise RuntimeError("视频流过大")
                        f.write(chunk)
            if not part.is_file() or part.stat().st_size == 0:
                part.unlink(missing_ok=True)
                return False
            part.replace(dest)
            return True
        except Exception:
            part.unlink(missing_ok=True)
            return False

    def _credentials(self) -> dict[str, str]:
        now = time.monotonic()
        if self._cred_cache and now < self._cred_expires:
            return self._cred_cache

        if self._credentials_getter is None:
            return {}
        try:
            cookies = self._credentials_getter()
        except Exception:
            return {}
        if not isinstance(cookies, Mapping):
            return {}

        result = {
            str(name): str(value)
            for name, value in cookies.items()
            if isinstance(name, str) and isinstance(value, str) and value
            and "\r" not in value and "\n" not in value
        }
        self._cred_cache = result
        self._cred_expires = now + 30.0
        return result

    def _cookie_header(self) -> str:
        return "; ".join(
            f"{name}={value}" for name, value in self._credentials().items()
        )

    def _base_headers(self, *, referer: str) -> dict[str, str]:
        headers = {
            "User-Agent": _USER_AGENT,
            "Referer": referer,
            "Origin": "https://www.bilibili.com",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        cookie_header = self._cookie_header()
        if cookie_header:
            headers["Cookie"] = cookie_header
        return headers

    def _stream_headers(self, bvid: str) -> dict[str, str]:
        headers = {
            "User-Agent": _USER_AGENT,
            "Referer": f"https://www.bilibili.com/video/{bvid}/",
            "Origin": "https://www.bilibili.com",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        cookie_header = self._cookie_header()
        if cookie_header:
            headers["Cookie"] = cookie_header
        return headers

    async def _request(
        self, url: str, params: Mapping[str, Any], *, wbi: bool
    ) -> dict[str, Any]:
        # 全局请求节流：保证两次请求之间至少间隔 _min_gap 秒
        if self._min_gap > 0:
            async with self._request_lock:
                now = time.monotonic()
                wait = self._min_gap - (now - self._last_request_ts)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_request_ts = time.monotonic()

        final_params = dict(params)
        if wbi:
            key = await self._get_wbi_key()
            final_params = _sign_params(final_params, key)

        headers = self._base_headers(referer="https://space.bilibili.com/")
        try:
            async with self._session.get(
                url, params=final_params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise BiliApiError(
                        f"B 站接口 HTTP {resp.status}：{text[:200]}",
                        code=resp.status,
                    )
                payload = json.loads(text)
        except BiliApiError:
            raise
        except aiohttp.ClientError as exc:
            raise BiliApiError(f"网络错误：{exc}") from exc
        except json.JSONDecodeError as exc:
            raise BiliApiError(f"B 站返回了非 JSON 数据：{exc}") from exc

        if not isinstance(payload, dict):
            raise BiliApiError("B 站返回了非 JSON 数据")
        code = _to_int(payload.get("code"), -1)
        if code != 0:
            if code == -352:
                raise BiliApiError(
                    "B 站风控校验失败（-352）。建议："
                    "① 等待 5-10 分钟后重试；"
                    "② 在浏览器打开一次 B 站动态页面手动过验证码；"
                    "③ 确认已扫码登录且 Cookie 未过期。",
                    code=code,
                )
            raise BiliApiError(
                f"B 站接口错误 {code}：{payload.get('message') or ''}",
                code=code,
            )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise BiliApiError("B 站返回了未知数据")
        return data

    async def _request_with_backoff(
        self,
        url: str,
        params: Mapping[str, Any],
        *,
        wbi: bool,
        max_attempts: int = 3,
    ) -> dict[str, Any]:
        """带指数退避的请求，仅对可重试错误码生效。

        适合列表类接口（fetch_*）；视频流解析和下载请用普通 _request。
        """
        last_exc: BiliApiError | None = None
        for attempt in range(max_attempts):
            try:
                return await self._request(url, params, wbi=wbi)
            except BiliApiError as exc:
                last_exc = exc
                if exc.code not in _RETRYABLE_CODES:
                    raise
                if attempt == max_attempts - 1:
                    break
                delay = _BACKOFF_DELAYS[min(attempt, len(_BACKOFF_DELAYS) - 1)]
                logger.warning(
                    "bili-subscription B 站限流（code=%s），%.0f 秒后重试（%d/%d）",
                    exc.code, delay, attempt + 1, max_attempts,
                )
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc

    async def _get_wbi_key(self) -> str:
        now = time.monotonic()
        if self._wbi_key and now < self._wbi_expires:
            return self._wbi_key
        async with self._wbi_lock:
            now = time.monotonic()
            if self._wbi_key and now < self._wbi_expires:
                return self._wbi_key
            headers = self._base_headers(referer="https://www.bilibili.com/")
            async with self._session.get(
                _NAV_URL, headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                payload = json.loads(await resp.text())
            data = payload.get("data") if isinstance(payload, dict) else None
            wbi = data.get("wbi_img") if isinstance(data, dict) else None
            if not isinstance(wbi, Mapping):
                raise BiliApiError("B 站未返回 WBI 密钥")
            key = _derive_mixin_key(
                str(wbi.get("img_url") or ""),
                str(wbi.get("sub_url") or ""),
            )
            self._wbi_key = key
            self._wbi_expires = time.monotonic() + _WBI_TTL
            return key


# ----------------------------------------------------------------------
# DASH 选择
# ----------------------------------------------------------------------
def _select_dash_audio(dash: Any) -> DashStream | None:
    if not isinstance(dash, Mapping):
        return None
    tracks = dash.get("audio")
    if not isinstance(tracks, list) or not tracks:
        return None
    cleaned = [t for t in tracks if isinstance(t, Mapping)]
    if not cleaned:
        return None
    target = 192_000
    below = [t for t in cleaned if _to_int(t.get("bandwidth")) <= target]
    pool = below or cleaned
    chosen = max(
        pool, key=lambda t: (_to_int(t.get("bandwidth")), _to_int(t.get("id")))
    )
    return _to_dash_stream(chosen, default_mime="audio/mp4")


def _select_dash_video(dash: Any) -> DashStream | None:
    if not isinstance(dash, Mapping):
        return None
    tracks = dash.get("video")
    if not isinstance(tracks, list) or not tracks:
        return None
    cleaned = [t for t in tracks if isinstance(t, Mapping)]
    if not cleaned:
        return None
    lowest_id = min(_to_int(t.get("id")) for t in cleaned)
    same_quality = [t for t in cleaned if _to_int(t.get("id")) == lowest_id]
    avc = [t for t in same_quality if "avc" in str(t.get("codecs") or "").lower()]
    pool = avc or same_quality
    chosen = min(
        pool, key=lambda t: (_to_int(t.get("bandwidth")), _to_int(t.get("id")))
    )
    return _to_dash_stream(chosen, default_mime="video/mp4")


def _single_progressive(durl: Any) -> DashStream | None:
    if not isinstance(durl, list) or len(durl) != 1 or not isinstance(durl[0], Mapping):
        return None
    return _to_dash_stream(durl[0], default_mime="video/mp4", url_keys=("url",))


def _to_dash_stream(
    raw: Mapping[str, Any], *,
    default_mime: str,
    url_keys: tuple[str, ...] = ("baseUrl", "base_url"),
) -> DashStream | None:
    url = ""
    for key in url_keys:
        candidate = str(raw.get(key) or "").strip()
        if candidate:
            url = candidate
            break
    if not url:
        return None
    backup_raw = raw.get("backupUrl") or raw.get("backup_url") or []
    backup_urls: list[str] = []
    if isinstance(backup_raw, list):
        for item in backup_raw:
            value = str(item or "").strip()
            if value and value != url:
                backup_urls.append(value)
    mime = str(raw.get("mimeType") or raw.get("mime_type") or default_mime).strip()
    codecs = str(raw.get("codecs") or raw.get("codec") or "").strip()
    return DashStream(
        url=url,
        backup_urls=tuple(backup_urls),
        mime_type=mime or default_mime,
        codecs=codecs,
    )


# ----------------------------------------------------------------------
# 解析辅助
# ----------------------------------------------------------------------
def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_duration(text: str) -> int:
    parts = text.strip().split(":")
    if not parts or not all(p.isdigit() for p in parts):
        return 0
    seconds = 0
    for p in parts:
        seconds = seconds * 60 + int(p)
    return seconds


def _parse_dynamic(raw: Any) -> UserDynamic | None:
    if not isinstance(raw, Mapping):
        return None
    dynamic_id = str(raw.get("id_str") or raw.get("id") or "").strip()
    if not dynamic_id:
        return None

    kind = str(raw.get("type") or "").strip()
    modules = raw.get("modules") if isinstance(raw.get("modules"), Mapping) else {}
    author = modules.get("module_author") if isinstance(modules, Mapping) else {}
    created_at = int(author.get("pub_ts") or 0) if isinstance(author, Mapping) else 0

    dynamic = modules.get("module_dynamic") if isinstance(modules, Mapping) else {}
    text = ""
    image_urls: list[str] = []

    if isinstance(dynamic, Mapping):
        desc = dynamic.get("desc")
        if isinstance(desc, Mapping):
            text = str(desc.get("text") or "").strip()
        major = dynamic.get("major")
        if isinstance(major, Mapping):
            draw = major.get("draw")
            if isinstance(draw, Mapping):
                items = draw.get("items")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, Mapping):
                            src = str(item.get("src") or "").strip()
                            if src:
                                if not src.startswith("http"):
                                    src = f"https:{src}"
                                image_urls.append(src)

    return UserDynamic(
        dynamic_id=dynamic_id,
        text=text,
        image_urls=tuple(image_urls[:6]),
        created_at=created_at,
        kind=kind,
        # 只认原创投稿视频动态，避免误伤"转发他人视频"的图文动态
        is_original_video=(kind == "DYNAMIC_TYPE_AV"),
    )