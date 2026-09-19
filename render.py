"""把动态/专栏内容渲染成一张 PNG。"""

from __future__ import annotations

import asyncio
import functools
import io
from pathlib import Path

from astrbot.api import logger

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_AVAILABLE = True
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


def pil_available() -> bool:
    return _PIL_AVAILABLE


@functools.lru_cache(maxsize=1)
def _warn_missing_font_once() -> None:
    logger.warning("bili-subscription 未找到中文字体，卡片可能显示方块")


@functools.lru_cache(maxsize=32)
def _load_font_cached(size: int, custom: str | None):
    candidates = [custom] if custom else []
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
    wrapped: list[str] = []
    for line in (body or "（无文字内容）").splitlines() or ["（无文字内容）"]:
        wrapped.extend(_wrap_text(line, body_font, max_text_width))
    body_height = len(wrapped) * line_height + 12

    scaled_images: list[Image.Image] = []
    image_area_height = 0
    for data in images:
        try:
            img = Image.open(io.BytesIO(data)).convert("RGB")
        except Exception:
            continue
        ratio = max_text_width / img.width
        new_w = max_text_width
        new_h = int(img.height * ratio)
        if new_h > _MAX_IMAGE_HEIGHT:
            ratio = _MAX_IMAGE_HEIGHT / img.height
            new_h = _MAX_IMAGE_HEIGHT
            new_w = int(img.width * ratio)
        scaled = img.resize((max(1, new_w), max(1, new_h)), Image.LANCZOS)
        scaled_images.append(scaled)
        image_area_height += scaled.height + 12

    total_height = (
        _PADDING + title_height + body_height
        + image_area_height + footer_height + _PADDING
    )
    canvas = Image.new("RGB", (_CARD_WIDTH, total_height), "#ffffff")
    draw = ImageDraw.Draw(canvas)

    y = _PADDING
    draw.text((_PADDING, y), title, font=title_font, fill="#1a1a1a")
    y += title_height
    for line in wrapped:
        draw.text((_PADDING, y), line, font=body_font, fill="#333333")
        y += line_height
    y += 12
    for img in scaled_images:
        canvas.paste(img, (_PADDING, y))
        y += img.height + 12
    if footer:
        draw.text((_PADDING, y), footer, font=footer_font, fill="#888888")

    buf = io.BytesIO()
    canvas.save(buf, format="PNG")
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