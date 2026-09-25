"""B 站 API 客户端：订阅抓取、视频解析、媒体下载。

防风控策略（多层）：
- 请求头补全（Origin / Accept-Language）；
- 每个客户端实例全程固定一个随机 UA——UA 与 buvid Cookie 是一对稳定
  指纹，真实浏览器不会中途换 UA，中途换反而制造"同设备多浏览器"矛盾；
- 全局节流带随机 jitter，避免机械间隔；
- 遇到风控时动态拉长节流间隔，连续 5 次成功才缓慢恢复；
- -352 时强制刷新 WBI 密钥（密钥过期会伪装成风控）；
- 风控熔断：连续多次风控错误后进入冷却期（默认 15 分钟），后台轮询
  完全停止请求，冷却结束自动恢复，避免被封期间继续硬刚加重风控；
- 指数退避重试（5s → 15s → 60s）；
- 图片下载独立 Semaphore 限并发；4xx 不重试、5xx 重试一次；
- Cookie 30 秒缓存 + bili_ticket 主动获取与每日续期。

动态解析说明：
B 站 feed 接口默认会把 opus 正文吞掉——必须同时满足两个条件才能拿到正文：
  ① 请求参数带 features=itemOpusStyle；
  ② 请求做 WBI 签名（wbi=True）。
fetch_dynamics 已同时满足；对"正文仍为空且动态含 opus/article/draw 结构"
的动态标记 may_need_detail，推送前再由 pusher 按需调用 enrich_dynamic()
用 detail 接口兜底一次——只有真正要推送的动态才会产生兜底请求，
把请求量降到最低。

此外，B 站会把置顶动态放在 items[0]，且可能是几个月前的旧动态。
fetch_dynamics 会在返回前按 created_at 倒序排序，避免 pusher 误把
置顶当最新。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import random
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp
from yarl import URL

from astrbot.api import logger


API_ORIGIN = "https://api.bilibili.com"
_NAV_URL = f"{API_ORIGIN}/x/web-interface/nav"
_DYNAMIC_URL = f"{API_ORIGIN}/x/polymer/web-dynamic/v1/feed/space"
_DYNAMIC_DETAIL_URL = f"{API_ORIGIN}/x/polymer/web-dynamic/v1/detail"
_VIDEO_LIST_URL = f"{API_ORIGIN}/x/space/wbi/arc/search"
_ARTICLE_URL = f"{API_ORIGIN}/x/space/wbi/article"
_ARTICLE_VIEW_URL = f"{API_ORIGIN}/x/article/view"
_VIEW_URL = f"{API_ORIGIN}/x/web-interface/wbi/view"
_PLAY_URL = f"{API_ORIGIN}/x/player/wbi/playurl"

# UA 池：都是真实存在的较新浏览器版本，避免老版本 UA 显得可疑
_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) "
    "Gecko/20100101 Firefox/135.0",
)

# 向后兼容：其他模块可能 import _USER_AGENT
_USER_AGENT = _USER_AGENTS[0]

_WBI_TTL = 600
_DEFAULT_VIDEO_QUALITY = 32
_IMAGE_MAX_BYTES = 8 * 1024 * 1024

# 让 feed 接口返回 opus 正文所必需的业务参数
_DYNAMIC_FEATURES = "itemOpusStyle"

# 专栏正文 HTML 里的插图（懒加载图 data-src 优先，src 兜底）
_ARTICLE_IMG_DATA_SRC_RE = re.compile(
    r'<img[^>]+?\bdata-src\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE
)
_ARTICLE_IMG_SRC_RE = re.compile(
    r'<img[^>]+?\bsrc\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE
)

# 节流参数
_DEFAULT_MIN_GAP = 1.5
_MIN_GAP_CEILING = 10.0     # 遇风控时拉长的上限
_JITTER_MAX = 0.8
_SPEED_UP_STREAK = 5        # 连续成功 N 次才恢复一档

# 风控熔断：连续多次风控错误后进入冷却期，后台完全停止请求
_RISK_CONTROL_CODES = (-352, -412, -509, -799)
_RISK_STREAK_THRESHOLD = 3
_RISK_COOLDOWN_SECONDS = 15 * 60

# 动态类型 → 中文标签（显示在卡片头部）
_KIND_CN = {
    "DYNAMIC_TYPE_DRAW": "图文动态",
    "DYNAMIC_TYPE_WORD": "文字动态",
    "DYNAMIC_TYPE_AV": "视频动态",
    "DYNAMIC_TYPE_ARTICLE": "专栏动态",
    "DYNAMIC_TYPE_FORWARD": "转发动态",
    "DYNAMIC_TYPE_MUSIC": "音乐动态",
    "DYNAMIC_TYPE_LIVE_RCMD": "直播动态",
    "DYNAMIC_TYPE_COMMON_SQUARE": "动态",
    "DYNAMIC_TYPE_LIVE": "直播动态",
    "DYNAMIC_TYPE_MEDIALIST": "合集动态",
    "DYNAMIC_TYPE_COURSES": "课程动态",
    "DYNAMIC_TYPE_COURSES_SEASON": "课程动态",
}

_MIXIN_INDICES = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 62, 6, 63, 57, 20, 34, 52, 59, 11, 36, 44,
)
_FILTER = str.maketrans("", "", "!'()*")

# -1 是本地网络错误（aiohttp ClientError 等），同样按指数退避重试
_RETRYABLE_CODES = frozenset({
    -1, -352, -412, -509, -799,
    408, 412, 429, 500, 502, 503, 504,
})

_BACKOFF_DELAYS = (5.0, 15.0, 60.0)

# bili_ticket 参数（防风控指纹，约 3 天有效，需定期刷新）
_TICKET_HMAC_KEY = b"ad1va46a7lza"
_TICKET_KEY_ID = "ec02"


def _gen_bili_ticket_sign(ts: int) -> str:
    """bili_ticket 的 HMAC 签名。"""
    msg = f"ts{ts}".encode("utf-8")
    return hmac.new(_TICKET_HMAC_KEY, msg, hashlib.sha256).hexdigest()


async def fetch_bili_ticket(session: aiohttp.ClientSession) -> dict[str, str]:
    """获取 bili_ticket，完善 Cookie 指纹，降低风控概率。

    失败不影响主流程，只是少一层防护。
    """
    ts = int(time.time())
    try:
        async with session.post(
            f"{API_ORIGIN}/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket",
            params={
                "key_id": _TICKET_KEY_ID,
                "hexsign": _gen_bili_ticket_sign(ts),
                "context[ts]": str(ts),
                "csrf": "",
            },
            headers={
                "User-Agent": _USER_AGENT,
                "Referer": "https://www.bilibili.com/",
                "Origin": "https://www.bilibili.com",
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            payload = await resp.json()
    except Exception as exc:
        logger.debug("bili-api 获取 bili_ticket 失败：%s", exc)
        return {}
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if not isinstance(data, dict):
        return {}
    ticket = str(data.get("ticket") or "").strip()
    expires = data.get("expires")
    if not ticket:
        return {}
    return {
        "bili_ticket": ticket,
        "bili_ticket_expires": str(expires or (ts + 259200)),
    }


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
    uploader: str = ""
    description: str = ""
    view_count: int = 0


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
    author_name: str = ""
    author_face: str = ""
    kind_cn: str = ""
    # 正文为空且原始结构疑似"正文被吞"时，推送前可做一次 detail 兜底
    may_need_detail: bool = False


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
    cover: str = ""
    description: str = ""
    view_count: int = 0
    pages: tuple[VideoPage, ...] = ()


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
        min_request_gap: float = _DEFAULT_MIN_GAP,
        video_quality: int = _DEFAULT_VIDEO_QUALITY,
    ) -> None:
        self._session = session
        self._credentials_getter = credentials_getter
        self._wbi_key: str | None = None
        self._wbi_expires = 0.0
        self._wbi_lock = asyncio.Lock()

        # 动态节流：base 是原始值，current 随风控压力伸缩
        self._base_min_gap = max(0.0, float(min_request_gap))
        self._min_gap = self._base_min_gap
        self._last_request_ts = 0.0
        self._request_lock = asyncio.Lock()

        # 连续成功计数器，用于控制 _speed_up 触发频率
        self._success_streak = 0

        # 图片下载限并发
        self._image_semaphore = asyncio.Semaphore(2)

        # 实例全程固定一个 UA：UA 与 buvid Cookie 是一对稳定指纹，
        # 真实浏览器不会中途换 UA，中途换反而制造指纹矛盾
        self._ua = random.choice(_USER_AGENTS)

        # 风控熔断状态
        self._risk_streak = 0
        self._cooldown_until = 0.0

        self._cred_cache: dict[str, str] = {}
        self._cred_expires = 0.0

        # 视频清晰度（qn），运行期可热更新
        self._video_quality = _DEFAULT_VIDEO_QUALITY
        self.set_video_quality(video_quality)

    def set_video_quality(self, qn: int) -> None:
        """热更新目标清晰度（qn），限制在合法区间 [16, 127]。"""
        try:
            value = int(qn)
        except (TypeError, ValueError):
            return
        self._video_quality = min(max(value, 16), 127)

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
                    uploader=str(raw.get("author") or "").strip(),
                    description=str(raw.get("description") or "").strip(),
                    view_count=int(raw.get("play") or 0),
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
            covers = list(_parse_article_covers(raw.get("image_urls")))
            banner = str(raw.get("banner_url") or "").strip()
            if banner and banner not in covers:
                covers.append(banner)
            result.append(
                UserArticle(
                    article_id=article_id,
                    title=str(raw.get("title") or "").strip(),
                    summary=str(raw.get("summary") or "").strip(),
                    covers=tuple(covers[:3]),
                    created_at=int(raw.get("publish_time") or 0),
                )
            )
        return result

    async def fetch_article_content_images(
        self, article_id: str, *, limit: int = 6
    ) -> tuple[str, ...]:
        """抓取专栏正文里的插图 URL（供卡片配图），失败返回空元组。

        只在新专栏需要推送时调用，避免无谓请求。
        该接口同样受风控影响，这里用带退避重试的请求，遇到 -352
        会拉长节流并重试，尽量避免正文插图抓取失败。
        """
        try:
            data = await self._request_with_backoff(
                _ARTICLE_VIEW_URL, {"id": article_id}, wbi=False
            )
        except BiliApiError as exc:
            logger.debug("bili-api 专栏正文 %s 抓取失败：%s", article_id, exc)
            return ()
        content = data.get("content") if isinstance(data, dict) else None
        if not isinstance(content, str) or not content:
            return ()

        urls: list[str] = []
        seen: set[str] = set()

        def _collect(candidate: str) -> bool:
            candidate = _unescape_img_url(candidate)
            if _is_usable_img_url(candidate) and candidate not in seen:
                seen.add(candidate)
                urls.append(candidate)
            return len(urls) >= limit

        # ① 接口直接返回的原图列表（最可靠，优先使用）
        origin = data.get("origin_image_urls")
        if isinstance(origin, list):
            for item in origin:
                if isinstance(item, str) and _collect(item):
                    return tuple(urls)

        # ② 从正文 HTML 里提取（懒加载 data-src 优先，src 兜底）
        for regex in (_ARTICLE_IMG_DATA_SRC_RE, _ARTICLE_IMG_SRC_RE):
            for match in regex.finditer(content):
                if _collect(match.group(1)):
                    return tuple(urls)
        return tuple(urls)

    async def fetch_dynamics(self, uid: str, *, limit: int = 5) -> list[UserDynamic]:
        # 关键点：
        # ① features=itemOpusStyle —— 否则 B 站不返回 opus 结构
        # ② wbi=True —— 否则 B 站把 opus.summary 吞掉，正文凭空消失
        data = await self._request_with_backoff(
            _DYNAMIC_URL,
            {
                "host_mid": uid,
                "timezone_offset": -480,
                "offset": "",
                "features": _DYNAMIC_FEATURES,
            },
            wbi=True,
        )
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []

        parsed_items: list[UserDynamic] = []
        for raw in items:
            parsed = _parse_dynamic(raw)
            if parsed is None:
                continue
            parsed_items.append(parsed)

        # 按发布时间从新到旧排序（置顶动态会打乱返回顺序）
        parsed_items.sort(key=lambda d: d.created_at, reverse=True)

        return parsed_items[:limit]

    async def enrich_dynamic(self, dynamic: UserDynamic) -> UserDynamic:
        """对"正文被吞"的动态用 detail 接口兜底，拿到正文后返回新对象。

        正文非空、或该动态没有值得兜底的结构时原样返回，不产生额外请求。
        """
        if dynamic.text or not dynamic.may_need_detail:
            return dynamic
        detail = await self._fetch_dynamic_detail(dynamic.dynamic_id)
        if detail is not None and detail.text:
            logger.debug(
                "bili-api 动态 %s 通过 detail 兜底拿到正文（%d 字）",
                dynamic.dynamic_id, len(detail.text),
            )
            return detail
        return dynamic

    async def _fetch_dynamic_detail(self, dynamic_id: str) -> UserDynamic | None:
        try:
            data = await self._request(
                _DYNAMIC_DETAIL_URL,
                {"id": dynamic_id, "features": _DYNAMIC_FEATURES},
                wbi=True,
            )
        except BiliApiError as exc:
            logger.debug("bili-api detail(%s) 失败：%s", dynamic_id, exc)
            return None
        item = data.get("item") if isinstance(data, dict) else None
        if not isinstance(item, Mapping):
            return None
        return _parse_dynamic(item)

    # ------------------------------------------------------------------
    # 登录态检测
    # ------------------------------------------------------------------
    async def check_login(self) -> bool:
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

    async def refresh_bili_ticket(self) -> bool:
        """刷新 bili_ticket（约 3 天有效，建议每天调用一次）。

        失败返回 False，不影响主流程。
        """
        try:
            cookies = await fetch_bili_ticket(self._session)
        except Exception as exc:
            logger.debug("bili-api 刷新 bili_ticket 失败：%s", exc)
            return False
        if not cookies:
            return False
        try:
            self._session.cookie_jar.update_cookies(
                cookies, response_url=URL("https://www.bilibili.com/")
            )
        except Exception as exc:
            logger.debug("bili-api 写入 bili_ticket 失败：%s", exc)
            return False
        logger.info("bili-api bili_ticket 已刷新")
        return True

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
        stat = data.get("stat")
        view_count = 0
        if isinstance(stat, Mapping):
            view_count = _to_int(stat.get("view"))
        return VideoDetail(
            bvid=resolved_bvid,
            title=str(data.get("title") or resolved_bvid).strip(),
            uploader=uploader or "未知上传者",
            cover=str(data.get("pic") or "").strip(),
            description=str(data.get("desc") or "").strip(),
            view_count=view_count,
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
                "qn": self._video_quality,
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
        video = _select_dash_video(dash, self._video_quality)
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
        """下载图片。

        - Semaphore(2) 限并发；
        - 4xx 立即放弃（不重试）；
        - 5xx / 网络异常重试一次（sleep 在 semaphore 外，避免阻塞其他下载）。
        """
        if not url:
            return None
        # B 站部分接口返回 // 开头的协议相对地址，补全为 https
        if url.startswith("//"):
            url = "https:" + url
        headers = self._base_headers(referer="https://www.bilibili.com/")

        for attempt in range(2):
            should_retry = False
            async with self._image_semaphore:
                try:
                    async with self._session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) > max_bytes:
                                return None
                            return data
                        if 400 <= resp.status < 500:
                            # 4xx 是确定性问题，重试无意义
                            return None
                        # 3xx 或 5xx 视为可重试
                        should_retry = True
                except Exception:
                    should_retry = True

            if should_retry and attempt == 0:
                await asyncio.sleep(1.5)
            elif not should_retry:
                break
        return None

    # ------------------------------------------------------------------
    # 内部：节流 / UA / 请求
    # ------------------------------------------------------------------
    async def _throttle(self) -> None:
        """全局节流：保证两次请求之间的最小间隔 + 随机抖动。

        抖动避免固定节奏被行为分析识别。
        """
        if self._min_gap <= 0:
            return
        async with self._request_lock:
            now = time.monotonic()
            wait = self._min_gap - (now - self._last_request_ts)
            if wait > 0:
                jitter = random.uniform(0.0, _JITTER_MAX)
                await asyncio.sleep(wait + jitter)
            self._last_request_ts = time.monotonic()

    def _slow_down(self) -> None:
        """遇到风控时拉长节流间隔。

        不再中途更换 UA：UA 与 buvid Cookie 是一对稳定指纹，
        换 UA 反而会制造"同一设备多个浏览器"的矛盾特征。
        """
        self._success_streak = 0
        new_gap = min(self._min_gap * 1.5, _MIN_GAP_CEILING)
        if new_gap > self._min_gap:
            logger.warning(
                "bili-api 遭遇风控，节流间隔 %.1fs → %.1fs",
                self._min_gap, new_gap,
            )
            self._min_gap = new_gap

    def _speed_up(self) -> None:
        """连续成功 N 次后才缓慢恢复节流间隔。"""
        if self._min_gap <= self._base_min_gap:
            return
        self._success_streak += 1
        if self._success_streak < _SPEED_UP_STREAK:
            return
        self._success_streak = 0
        new_gap = max(self._base_min_gap, self._min_gap * 0.8)
        if new_gap < self._min_gap:
            self._min_gap = new_gap

    def _pick_ua(self) -> str:
        return self._ua

    def _note_risk_error(self) -> None:
        """记录一次风控错误；连续多次后进入冷却期（熔断）。"""
        self._risk_streak += 1
        if self._risk_streak >= _RISK_STREAK_THRESHOLD:
            self._risk_streak = 0
            self._cooldown_until = time.monotonic() + _RISK_COOLDOWN_SECONDS
            logger.warning(
                "bili-api 连续遭遇风控，进入 %.0f 分钟冷却期，"
                "期间后台轮询停止请求，冷却结束自动恢复",
                _RISK_COOLDOWN_SECONDS / 60,
            )

    def in_cooldown(self) -> bool:
        """是否处于风控冷却期（熔断中）。"""
        return time.monotonic() < self._cooldown_until

    def cooldown_remaining_seconds(self) -> int:
        """冷却期剩余秒数；不在冷却期返回 0。"""
        if not self.in_cooldown():
            return 0
        return int(self._cooldown_until - time.monotonic()) + 1

    def current_min_gap(self) -> float:
        """当前实际节流间隔（遭遇风控时会拉长）。"""
        return self._min_gap

    def base_min_gap(self) -> float:
        """配置的基准节流间隔。"""
        return self._base_min_gap

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
            "User-Agent": self._pick_ua(),
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
            "User-Agent": self._pick_ua(),
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
        # 先解析 WBI 签名（必要时会先请求 _NAV_URL 拿密钥，
        # 该请求本身也会经过 _throttle），再在真正发请求前节流一次。
        final_params = dict(params)
        if wbi:
            key = await self._get_wbi_key()
            final_params = _sign_params(final_params, key)

        headers = self._base_headers(referer="https://space.bilibili.com/")
        await self._throttle()
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
        last_exc: BiliApiError | None = None
        for attempt in range(max_attempts):
            try:
                result = await self._request(url, params, wbi=wbi)
                # 成功：重置风控计数 + 达到阈值才恢复节流间隔
                self._risk_streak = 0
                self._speed_up()
                return result
            except BiliApiError as exc:
                last_exc = exc
                if exc.code not in _RETRYABLE_CODES:
                    raise
                # 风控专属处理：拉长节流 + 熔断计数 + 强制刷新 WBI
                if exc.code in _RISK_CONTROL_CODES:
                    self._slow_down()
                    self._note_risk_error()
                    if exc.code == -352:
                        # WBI 密钥可能已过期，强制下次重新获取
                        self._wbi_key = None
                        self._wbi_expires = 0.0
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
            await self._throttle()
            try:
                async with self._session.get(
                    _NAV_URL, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status >= 400:
                        raise BiliApiError(
                            f"获取 WBI 密钥失败：HTTP {resp.status}",
                            code=resp.status,
                        )
                    payload = json.loads(await resp.text())
            except BiliApiError:
                raise
            except (aiohttp.ClientError, json.JSONDecodeError) as exc:
                # 包装成 BiliApiError，让 _request_with_backoff 正常重试
                raise BiliApiError(f"获取 WBI 密钥失败：{exc}") from exc
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


def _select_dash_video(dash: Any, target_qn: int = _DEFAULT_VIDEO_QUALITY) -> DashStream | None:
    """从 DASH 里选不超过 target_qn 的最高可用清晰度。

    同清晰度内优先 AVC（兼容性最好）、再选码率最低的轨道，控制体积。
    """
    if not isinstance(dash, Mapping):
        return None
    tracks = dash.get("video")
    if not isinstance(tracks, list) or not tracks:
        return None
    cleaned = [t for t in tracks if isinstance(t, Mapping)]
    if not cleaned:
        return None

    def qn_of(track: Mapping[str, Any]) -> int:
        return _to_int(track.get("id"))

    eligible = [t for t in cleaned if 0 < qn_of(t) <= target_qn]
    if not eligible:
        # 没有不超过目标的：取最低可用清晰度兜底
        lowest = min(qn_of(t) for t in cleaned)
        pool = [t for t in cleaned if qn_of(t) == lowest]
    else:
        highest = max(qn_of(t) for t in eligible)
        pool = [t for t in eligible if qn_of(t) == highest]
    avc = [t for t in pool if "avc" in str(t.get("codecs") or "").lower()]
    candidates = avc or pool
    chosen = min(
        candidates, key=lambda t: (_to_int(t.get("bandwidth")), _to_int(t.get("id")))
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


def _parse_article_covers(raw_covers: Any) -> tuple[str, ...]:
    """解析专栏封面列表：兼容字符串与 {url: ...} 两种结构。"""
    if not isinstance(raw_covers, list):
        return ()
    covers: list[str] = []
    for item in raw_covers[:3]:
        if isinstance(item, str):
            url = item.strip()
        elif isinstance(item, Mapping):
            url = str(item.get("url") or "").strip()
        else:
            continue
        if url:
            covers.append(url)
    return tuple(covers)


def _is_usable_img_url(url: str) -> bool:
    """过滤 data:/blob: 等不可下载的图片地址。"""
    if not url:
        return False
    lowered = url.casefold()
    if lowered.startswith(("data:", "blob:", "javascript:")):
        return False
    return (
        url.startswith("http://")
        or url.startswith("https://")
        or url.startswith("//")
    )


def _unescape_img_url(url: str) -> str:
    """还原 URL 里的 HTML 实体（正文里的 &amp; 等），否则下载会 404。"""
    return (
        url.strip()
        .replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&#x26;", "&")
    )


def _append_image_url(target: list[str], raw: Any) -> None:
    src = str(raw or "").strip()
    if not src:
        return
    if not src.startswith("http"):
        src = f"https:{src}"
    target.append(src)


def _extract_opus_text(opus: Mapping[str, Any]) -> str:
    title = str(opus.get("title") or "").strip()

    summary_text = ""
    summary = opus.get("summary")
    if isinstance(summary, Mapping):
        summary_text = str(summary.get("text") or "").strip()
        if not summary_text:
            nodes = summary.get("rich_text_nodes")
            if isinstance(nodes, list):
                parts: list[str] = []
                for node in nodes:
                    if isinstance(node, Mapping):
                        orig = str(node.get("orig_text") or "")
                        if orig:
                            parts.append(orig)
                summary_text = "".join(parts).strip()

    if title and summary_text:
        return f"{title}\n\n{summary_text}"
    return title or summary_text


def _maybe_needs_detail(raw: Any) -> bool:
    """判断这条原始动态是否值得调 detail 兜底。

    只有包含以下 major 结构的动态才可能有"正文被吞"的问题：
    opus / article / draw。其他类型本来就没正文，不需要兜底。
    """
    if not isinstance(raw, Mapping):
        return False
    modules = raw.get("modules")
    if not isinstance(modules, Mapping):
        return False
    dynamic = modules.get("module_dynamic")
    if not isinstance(dynamic, Mapping):
        return False
    major = dynamic.get("major")
    if not isinstance(major, Mapping):
        return False
    return any(
        isinstance(major.get(key), Mapping)
        for key in ("opus", "article", "draw")
    )


def _parse_dynamic(raw: Any, _depth: int = 0) -> UserDynamic | None:
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
        # 1) 旧结构：module_dynamic.desc.text
        desc = dynamic.get("desc")
        if isinstance(desc, Mapping):
            text = str(desc.get("text") or "").strip()

        major = dynamic.get("major")
        if isinstance(major, Mapping):
            # 2) opus
            opus = major.get("opus")
            if isinstance(opus, Mapping):
                opus_text = _extract_opus_text(opus)
                if opus_text and not text:
                    text = opus_text
                pics = opus.get("pics")
                if isinstance(pics, list):
                    for pic in pics:
                        if isinstance(pic, Mapping):
                            _append_image_url(image_urls, pic.get("url"))

            # 3) 专栏卡片
            article = major.get("article")
            if isinstance(article, Mapping):
                title = str(article.get("title") or "").strip()
                summary = str(article.get("desc") or "").strip()
                if title and summary:
                    article_text = f"{title}\n\n{summary}"
                else:
                    article_text = title or summary
                if article_text and not text:
                    text = article_text
                covers = article.get("covers")
                if isinstance(covers, list):
                    for c in covers:
                        _append_image_url(image_urls, c)

            # 4) 图文动态
            draw = major.get("draw")
            if isinstance(draw, Mapping):
                items = draw.get("items")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, Mapping):
                            _append_image_url(image_urls, item.get("src"))

    # 转发动态：把原动态的文字/图片一并带出（只解析一层，防止递归爆炸）
    if kind == "DYNAMIC_TYPE_FORWARD" and _depth < 1:
        orig_raw = raw.get("orig")
        if isinstance(orig_raw, Mapping):
            orig_parsed = _parse_dynamic(orig_raw, _depth + 1)
            if orig_parsed is not None:
                if not text:
                    text = orig_parsed.text
                if not image_urls:
                    image_urls = list(orig_parsed.image_urls)

    # 去重
    seen: set[str] = set()
    unique_images: list[str] = []
    for url in image_urls:
        if url not in seen:
            seen.add(url)
            unique_images.append(url)

    # 作者信息
    author_name = ""
    author_face = ""
    if isinstance(author, Mapping):
        author_name = str(author.get("name") or "").strip()
        author_face = str(author.get("face") or "").strip()
        if author_face and not author_face.startswith("http"):
            author_face = f"https:{author_face}"

    return UserDynamic(
        dynamic_id=dynamic_id,
        text=text,
        image_urls=tuple(unique_images[:6]),
        created_at=created_at,
        kind=kind,
        is_original_video=(kind == "DYNAMIC_TYPE_AV"),
        author_name=author_name,
        author_face=author_face,
        kind_cn=_KIND_CN.get(kind, "动态"),
        # 正文为空且结构疑似"正文被吞"时，推送前可做一次 detail 兜底
        may_need_detail=(not text and _maybe_needs_detail(raw)),
    )