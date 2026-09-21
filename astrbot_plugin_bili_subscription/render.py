"""把动态/专栏内容渲染成 PNG/JPEG。

提供两个渲染函数：
- render_card          —— 通用卡片（标题 + 正文 + 图片），给专栏用
- render_dynamic_card  —— B 站动态样式（头像 + 昵称 + 时间 + 类型 + 正文 + 图片）

对用户填写的字体路径做规范化处理，避免编辑过程中误带入的换行/转义字符
导致字体静默回落到默认字体。

图片处理管线（性能/画质最优）：
1. 每张嵌入图只等比缩放，不做单独压缩，避免二次有损编码；
2. 画布统一渲染完后，一次性 JPEG 编码；
3. 若超过 _CARD_MAX_BYTES，逐步降低 quality 重编码；
4. 纯文字卡片用 PNG，保证文字锐利。

这样单张图不再有独立上限，整张卡片有统一上限兜底。
"""

from __future__ import annotations

import asyncio
import functools
import io
from datetime import datetime
from pathlib import Path

from astrbot.api import logger

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_AVAILABLE = True
    # Pillow 10+ 推荐 Image.Resampling，旧版本在 Image 上直接有常量
    _LANCZOS = getattr(Image, "Resampling", Image).LANCZOS
except ModuleNotFoundError:
    _PIL_AVAILABLE = False


_FONT_CANDIDATES = (
    "C:/Windows/Fonts/msyh.ttc",
    "C:/Windows/Fonts/simhei.ttf",
    "C:/Windows/Fonts/simsun.ttc",
    "/System/Library/Fonts/PingFang.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
)

_CARD_WIDTH = 720
_PADDING = 24
_MAX_IMAGE_HEIGHT = 900

# 整张卡片（含所有嵌入图）的字节上限
_CARD_MAX_BYTES = 2 * 1024 * 1024

# 卡片总高度上限：过高的图部分平台会拒收或压缩失真
_CARD_MAX_HEIGHT = 4096

# 正文最大字符数：防止超长文本把卡片撑爆、拖慢渲染
_BODY_MAX_CHARS = 800
_TRUNCATION_NOTE = "\n\n……（正文过长，已截断）"

# 从高到低的 quality 序列；依次尝试直到卡片大小达标
_JPEG_QUALITY_STEPS = (88, 80, 72, 64, 56, 48)

# B 站深色主题配色
_BG_COLOR = "#18191c"
_PRIMARY_TEXT = "#e7e9ec"
_SECONDARY_TEXT = "#9499a0"
_DIVIDER = "#2a2b2f"


def pil_available() -> bool:
    return _PIL_AVAILABLE


@functools.lru_cache(maxsize=1)
def _warn_missing_font_once() -> None:
    logger.warning("bili-subscription 未找到中文字体，卡片可能显示方块")


def _normalize_font_path(raw: str | None) -> str | None:
    """清洗用户填写的字体路径。"""
    if not raw:
        return None
    p = str(raw).strip()
    if len(p) >= 2 and p[0] == p[-1] and p[0] in ("'", '"'):
        p = p[1:-1]
    p = p.replace("\\n", "").replace("\\r", "").replace("\\t", "")
    p = p.replace("\r", "").replace("\n", "").replace("\t", "")
    p = p.strip()
    return p or None


@functools.lru_cache(maxsize=32)
def _load_font_cached(size: int, custom: str | None):
    candidates: list[str] = []
    normalized = _normalize_font_path(custom)
    if normalized:
        candidates.append(normalized)
    candidates.extend(_FONT_CANDIDATES)

    for path in candidates:
        if path and Path(path).exists():
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    _warn_missing_font_once()
    try:
        return ImageFont.load_default()
    except Exception:
        return None


def _normalize_text(raw: str) -> str:
    """把字面的 \\n 还原成真换行。"""
    if not raw:
        return ""
    text = raw.replace("\\r\\n", "\n")
    text = text.replace("\\n", "\n").replace("\\r", "")
    return text


def _truncate_body(raw: str) -> str:
    """规范化 + 截断超长正文，保证卡片高度与渲染耗时可控。"""
    text = _normalize_text(raw) or "（无文字内容）"
    if len(text) > _BODY_MAX_CHARS:
        text = text[:_BODY_MAX_CHARS] + _TRUNCATION_NOTE
    return text


def _format_time(ts: int) -> str:
    if ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except (OSError, ValueError, OverflowError):
        return ""


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    """把图片裁成圆形，用于头像。"""
    img = img.convert("RGBA").resize((size, size), _LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    result = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    result.paste(img, (0, 0), mask)
    return result


# ----------------------------------------------------------------------
# 通用卡片（专栏用）：标题 + 正文 + 图片
# ----------------------------------------------------------------------
async def render_card(
    *,
    title: str,
    body: str,
    images: list[bytes],
    footer: str = "",
    font_path: str | None = None,
) -> bytes | None:
    if not _PIL_AVAILABLE:
        return None
    return await asyncio.to_thread(
        _render_sync, title, body, images, footer, font_path
    )


def _render_sync(
    title: str, body: str, images: list[bytes],
    footer: str, font_path: str | None,
) -> bytes | None:
    title_font = _load_font_cached(28, font_path)
    body_font = _load_font_cached(20, font_path)
    footer_font = _load_font_cached(14, font_path)
    if title_font is None or body_font is None:
        return None

    line_height = _line_height(body_font)
    title_height = _line_height(title_font) + 12
    footer_height = _line_height(footer_font) + 12 if footer else 0

    max_text_width = _CARD_WIDTH - 2 * _PADDING
    normalized_body = _truncate_body(body)

    wrapped: list[str] = []
    for line in normalized_body.splitlines() or ["（无文字内容）"]:
        wrapped.extend(_wrap_text(line, body_font, max_text_width))
    body_height = len(wrapped) * line_height + 12

    fixed_height = _PADDING + title_height + body_height + footer_height + _PADDING
    image_budget = max(0, _CARD_MAX_HEIGHT - fixed_height)
    scaled_images, image_area_height = _prepare_images(
        images, max_text_width, height_budget=image_budget or None
    )

    total_height = fixed_height + image_area_height
    canvas = Image.new("RGB", (_CARD_WIDTH, total_height), _BG_COLOR)
    draw = ImageDraw.Draw(canvas)

    y = _PADDING
    draw.text((_PADDING, y), title, font=title_font, fill=_PRIMARY_TEXT)
    y += title_height
    for line in wrapped:
        draw.text((_PADDING, y), line, font=body_font, fill=_PRIMARY_TEXT)
        y += line_height
    y += 12
    for img in scaled_images:
        canvas.paste(img, (_PADDING, y))
        y += img.height + 12
    if footer:
        draw.text((_PADDING, y), footer, font=footer_font, fill=_SECONDARY_TEXT)

    return _save_canvas(canvas, has_images=bool(scaled_images))


# ----------------------------------------------------------------------
# 动态卡片（B 站风格）：头像 + 昵称 + 时间 + 类型 + 正文 + 图片
# ----------------------------------------------------------------------
async def render_dynamic_card(
    *,
    author_name: str,
    author_face: bytes | None,
    kind_cn: str,
    timestamp: int,
    body: str,
    images: list[bytes],
    footer: str = "",
    font_path: str | None = None,
) -> bytes | None:
    if not _PIL_AVAILABLE:
        return None
    return await asyncio.to_thread(
        _render_dynamic_sync,
        author_name, author_face, kind_cn, timestamp,
        body, images, footer, font_path,
    )


def _render_dynamic_sync(
    author_name: str,
    author_face: bytes | None,
    kind_cn: str,
    timestamp: int,
    body: str,
    images: list[bytes],
    footer: str,
    font_path: str | None,
) -> bytes | None:
    name_font = _load_font_cached(22, font_path)
    meta_font = _load_font_cached(14, font_path)
    body_font = _load_font_cached(20, font_path)
    footer_font = _load_font_cached(14, font_path)
    if name_font is None or body_font is None:
        return None

    avatar_size = 56
    header_height = avatar_size + _PADDING * 2   # 上下各留一个 padding

    max_text_width = _CARD_WIDTH - 2 * _PADDING

    # ---- 正文换行 ----
    normalized_body = _truncate_body(body)
    body_line_height = _line_height(body_font)
    wrapped: list[str] = []
    for line in normalized_body.splitlines() or ["（无文字内容）"]:
        wrapped.extend(_wrap_text(line, body_font, max_text_width))
    body_height = len(wrapped) * body_line_height + 12

    # ---- 页脚 ----
    footer_height = _line_height(footer_font) + 12 if footer else 0

    # ---- 图片缩放（受卡片总高上限约束） ----
    fixed_height = header_height + body_height + footer_height + _PADDING
    image_budget = max(0, _CARD_MAX_HEIGHT - fixed_height)
    scaled_images, image_area_height = _prepare_images(
        images, max_text_width, height_budget=image_budget or None
    )

    # ---- 总高 ----
    total_height = fixed_height + image_area_height
    canvas = Image.new("RGB", (_CARD_WIDTH, total_height), _BG_COLOR)
    draw = ImageDraw.Draw(canvas)

    # ---- 头部：头像 + 昵称 + 时间 + 类型 ----
    avatar_x = _PADDING
    avatar_y = _PADDING
    avatar = _load_avatar(author_face, avatar_size)
    if avatar is not None:
        canvas.paste(avatar, (avatar_x, avatar_y), avatar)
    else:
        draw.ellipse(
            (avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size),
            fill="#2a2b2f",
        )

    text_x = avatar_x + avatar_size + 16
    name_y = avatar_y + 2
    draw.text(
        (text_x, name_y),
        author_name or "未知作者",
        font=name_font,
        fill=_PRIMARY_TEXT,
    )

    meta_parts: list[str] = []
    if kind_cn:
        meta_parts.append(kind_cn)
    time_str = _format_time(timestamp)
    if time_str:
        meta_parts.append(time_str)
    meta_text = "  ·  ".join(meta_parts)
    if meta_text:
        meta_y = name_y + _line_height(name_font) + 4
        draw.text((text_x, meta_y), meta_text, font=meta_font, fill=_SECONDARY_TEXT)

    # 头部分割线
    divider_y = header_height - 1
    draw.line(
        [(_PADDING, divider_y), (_CARD_WIDTH - _PADDING, divider_y)],
        fill=_DIVIDER, width=1,
    )

    # ---- 正文 ----
    y = header_height
    for line in wrapped:
        draw.text((_PADDING, y), line, font=body_font, fill=_PRIMARY_TEXT)
        y += body_line_height
    y += 12

    # ---- 图片 ----
    for img in scaled_images:
        canvas.paste(img, (_PADDING, y))
        y += img.height + 12

    # ---- 页脚 ----
    if footer:
        draw.text((_PADDING, y), footer, font=footer_font, fill=_SECONDARY_TEXT)

    return _save_canvas(canvas, has_images=bool(scaled_images))


def _load_avatar(data: bytes | None, size: int) -> Image.Image | None:
    if not data:
        return None
    try:
        img = Image.open(io.BytesIO(data))
    except Exception:
        return None
    try:
        return _circle_crop(img, size)
    except Exception:
        return None


def _prepare_images(
    images: list[bytes],
    max_width: int,
    *,
    height_budget: int | None = None,
) -> tuple[list[Image.Image], int]:
    """只做缩放，不做单独压缩。

    这样最终只有一次 JPEG 编码（_save_canvas 里的），画质和 CPU 都更优。
    height_budget 是图片区域（含间距）的总高预算；超预算时整体等比缩小，
    保证卡片总高不会超过平台拒收的尺寸。
    """
    prepared: list[tuple[Image.Image, int, int]] = []
    total_height = 0
    for data in images:
        try:
            img = Image.open(io.BytesIO(data)).convert("RGB")
        except Exception:
            continue

        # 先只计算目标尺寸，最后统一 resize 一次（避免二次缩放失真）
        w = min(img.width, max_width)
        h = max(1, int(img.height * w / img.width))

        # 单张图高不超过 _MAX_IMAGE_HEIGHT
        if h > _MAX_IMAGE_HEIGHT:
            ratio = _MAX_IMAGE_HEIGHT / h
            h = _MAX_IMAGE_HEIGHT
            w = max(1, int(w * ratio))

        prepared.append((img, w, h))
        total_height += h + 12

    factor = 1.0
    if height_budget is not None and total_height > height_budget > 0:
        factor = height_budget / total_height

    result: list[Image.Image] = []
    final_height = 0
    for img, w, h in prepared:
        new_w, new_h = max(1, int(w * factor)), max(1, int(h * factor))
        if (new_w, new_h) != img.size:
            img = img.resize((new_w, new_h), _LANCZOS)
        result.append(img)
        final_height += new_h + 12
    return result, final_height


def _save_canvas(canvas: Image.Image, *, has_images: bool) -> bytes:
    """保存画布。

    - 有图片时用 JPEG，quality 从高到低尝试直到卡片大小达标；
    - 纯文字时用 PNG（文字锐利且体积小）。
    """
    buf = io.BytesIO()
    if has_images:
        data = b""
        for quality in _JPEG_QUALITY_STEPS:
            buf.seek(0)
            buf.truncate()
            canvas.save(buf, format="JPEG", quality=quality, optimize=True)
            data = buf.getvalue()
            if len(data) <= _CARD_MAX_BYTES:
                return data
        # 最低 quality 仍超标：直接返回，交给上层发送时判断
        return data

    canvas.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _line_height(font) -> int:
    try:
        ascent, descent = font.getmetrics()
        return ascent + descent + 4
    except Exception:
        return 24


def _wrap_text(text: str, font, max_width: int) -> list[str]:
    if not text:
        return [""]
    if max_width < 1:
        max_width = 1

    lines: list[str] = []
    current = ""
    for ch in text:
        if _text_width(ch, font) > max_width:
            if current:
                lines.append(current)
                current = ""
            lines.append(ch)
            continue
        candidate = current + ch
        if _text_width(candidate, font) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = ch
    if current:
        lines.append(current)
    return lines or [""]


def _text_width(text: str, font) -> int:
    try:
        bbox = font.getbbox(text)
        return max(1, bbox[2] - bbox[0])
    except Exception:
        return max(1, len(text) * 14)