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
import re
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


# 主字体（中文优先）
_FONT_CANDIDATES = (
    "C:/Windows/Fonts/msyh.ttc",
    "C:/Windows/Fonts/simhei.ttf",
    "C:/Windows/Fonts/simsun.ttc",
    "/System/Library/Fonts/PingFang.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
)

# 韩文回退字体（存在才加载；主字体缺韩文字形时自动接管）
_HANGUL_FONT_CANDIDATES = (
    "C:/Windows/Fonts/malgun.ttf",
    "/System/Library/Fonts/AppleSDGothicNeo.ttc",
    "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
)

# 日文回退字体（存在才加载；主字体缺假名字形时自动接管）
_KANA_FONT_CANDIDATES = (
    "C:/Windows/Fonts/YuGothM.ttc",
    "C:/Windows/Fonts/meiryo.ttc",
    "C:/Windows/Fonts/msgothic.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
)

# emoji/符号回退字体（PIL 会渲染成单色轮廓，存在才加载）
_EMOJI_FONT_CANDIDATES = (
    "C:/Windows/Fonts/seguiemj.ttf",
    "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
)

# 字体栈最多加载的字体数量（控制启动与渲染开销）
_FONT_STACK_MAX = 8

# 扫描系统字体目录时，视为"可能支持韩/日文"的文件名特征
_EXTRA_FONT_PATTERN = re.compile(
    r"malgun|gulim|batang|dotum|gungsuh|nanum|yugoth|meiryo|msgothic|msmincho"
    r"|noto|sourcehan|source-han|sarasa|unifont|wqy|korean|japanese"
)

# 用于探测字体 .notdef 字形的非字符（几乎所有字体都没有）
_MISSING_CHAR = "\U0010FFFF"
_notdef_bbox_cache: dict[int, tuple] = {}

_CARD_WIDTH = 720
_PADDING = 24
# 单张图的最大高度：防止极端长图把卡片撑爆。
# 一般长截图（如 1080×4000）缩到卡片宽 672 后约 2400 高，仍能完整清晰显示，
# 而不是被压成 ~200px 宽的小条导致文字看不清。
_MAX_IMAGE_HEIGHT = 3000

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


class _FontStack:
    """多字体栈：主字体 + 韩文/日文/emoji 回退字体。

    pick(ch) 选择第一个真正包含该字符字形的字体（通过与 .notdef 字形对比判定），
    解决单一中文字体缺韩文/日文/emoji 字形时渲染成方框的问题。
    """

    __slots__ = ("fonts", "primary", "_pick_cache")

    def __init__(self, fonts: tuple) -> None:
        self.fonts = fonts
        self.primary = fonts[0] if fonts else None
        self._pick_cache: dict[str, object] = {}

    def pick(self, ch: str):
        if len(self.fonts) <= 1:
            return self.primary
        cached = self._pick_cache.get(ch)
        if cached is not None:
            return cached
        chosen = self._pick_uncached(ch)
        if len(self._pick_cache) >= 2048:
            self._pick_cache.clear()
        self._pick_cache[ch] = chosen
        return chosen

    def _pick_uncached(self, ch: str):
        for font in self.fonts:
            if _font_has_char(font, ch):
                return font
        return self.primary

    def line_height(self) -> int:
        heights = [_font_metrics_height(f) for f in self.fonts]
        return max(heights, default=24)


def _font_has_char(font, ch: str) -> bool:
    """判断字体是否包含某字符。

    字体缺失字符时 FreeType 渲染的是 .notdef 空框，其 bbox 与真实字形不同，
    据此区分；异常时按"有字形"处理（回退到主字体绘制）。
    """
    if font is None or ch == _MISSING_CHAR:
        return False
    try:
        key = id(font)
        notdef = _notdef_bbox_cache.get(key)
        if notdef is None:
            notdef = font.getbbox(_MISSING_CHAR)
            _notdef_bbox_cache[key] = notdef
        return font.getbbox(ch) != notdef
    except Exception:
        return True


@functools.lru_cache(maxsize=1)
def _scan_font_files() -> tuple[str, ...]:
    """扫描系统字体目录里疑似支持韩文/日文/emoji 的字体文件。

    跳过 Bold/Light 等变体（文件名以 bd/bold/b 结尾），
    避免混排时部分字符比主字体明显加粗。
    """
    try:
        font_dir = Path("C:/Windows/Fonts")
        if not font_dir.is_dir():
            return ()
        results: list[str] = []
        for path in font_dir.iterdir():
            if path.suffix.casefold() not in (".ttf", ".ttc", ".otf"):
                continue
            stem = path.stem.casefold()
            if re.search(r"(bd|bold|light|regular|-b|_b)$", stem):
                continue
            if _EXTRA_FONT_PATTERN.search(stem):
                results.append(str(path))
        results.sort()
        return tuple(results)
    except OSError:
        return ()


@functools.lru_cache(maxsize=32)
def _load_font_stack(size: int, custom: str | None) -> _FontStack:
    """加载主字体 + 韩/日/emoji 回退字体，组成字体栈（按字号缓存）。"""
    normalized = _normalize_font_path(custom)
    ordered: list[str] = []
    seen: set[str] = set()

    def add(path: str | None) -> None:
        if not path:
            return
        key = path.casefold()
        if key not in seen:
            seen.add(key)
            ordered.append(path)

    add(normalized)
    for path in _FONT_CANDIDATES:
        add(path)
    for path in _HANGUL_FONT_CANDIDATES:
        add(path)
    for path in _KANA_FONT_CANDIDATES:
        add(path)
    for path in _EMOJI_FONT_CANDIDATES:
        add(path)
    for path in _scan_font_files():
        add(path)

    fonts: list = []
    for path in ordered:
        if not Path(path).exists():
            continue
        try:
            fonts.append(ImageFont.truetype(path, size))
        except OSError:
            continue
        if len(fonts) >= _FONT_STACK_MAX:
            break

    if not fonts:
        _warn_missing_font_once()
        try:
            default = ImageFont.load_default()
        except Exception:
            default = None
        if default is not None:
            fonts.append(default)
    return _FontStack(tuple(fonts))


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
    title_stack = _load_font_stack(28, font_path)
    body_stack = _load_font_stack(20, font_path)
    footer_stack = _load_font_stack(14, font_path)
    if title_stack.primary is None or body_stack.primary is None:
        return None

    line_height = _line_height(body_stack)
    title_height = _line_height(title_stack) + 12
    footer_height = _line_height(footer_stack) + 12 if footer else 0

    max_text_width = _CARD_WIDTH - 2 * _PADDING
    normalized_body = _truncate_body(body)

    wrapped: list[str] = []
    for line in normalized_body.splitlines() or ["（无文字内容）"]:
        wrapped.extend(_wrap_text(line, body_stack, max_text_width))
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
    _draw_text(draw, (_PADDING, y), title, title_stack, _PRIMARY_TEXT)
    y += title_height
    for line in wrapped:
        _draw_text(draw, (_PADDING, y), line, body_stack, _PRIMARY_TEXT)
        y += line_height
    y += 12
    for img in scaled_images:
        canvas.paste(img, (_PADDING, y))
        y += img.height + 12
    if footer:
        _draw_text(draw, (_PADDING, y), footer, footer_stack, _SECONDARY_TEXT)

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
    name_stack = _load_font_stack(22, font_path)
    meta_stack = _load_font_stack(14, font_path)
    body_stack = _load_font_stack(20, font_path)
    footer_stack = _load_font_stack(14, font_path)
    if name_stack.primary is None or body_stack.primary is None:
        return None

    avatar_size = 56
    header_height = avatar_size + _PADDING * 2   # 上下各留一个 padding

    max_text_width = _CARD_WIDTH - 2 * _PADDING

    # ---- 正文换行 ----
    normalized_body = _truncate_body(body)
    body_line_height = _line_height(body_stack)
    wrapped: list[str] = []
    for line in normalized_body.splitlines() or ["（无文字内容）"]:
        wrapped.extend(_wrap_text(line, body_stack, max_text_width))
    body_height = len(wrapped) * body_line_height + 12

    # ---- 页脚 ----
    footer_height = _line_height(footer_stack) + 12 if footer else 0

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
    _draw_text(
        draw, (text_x, name_y), author_name or "未知作者",
        name_stack, _PRIMARY_TEXT,
    )

    meta_parts: list[str] = []
    if kind_cn:
        meta_parts.append(kind_cn)
    time_str = _format_time(timestamp)
    if time_str:
        meta_parts.append(time_str)
    meta_text = "  ·  ".join(meta_parts)
    if meta_text:
        meta_y = name_y + _line_height(name_stack) + 4
        _draw_text(
            draw, (text_x, meta_y), meta_text, meta_stack, _SECONDARY_TEXT
        )

    # 头部分割线
    divider_y = header_height - 1
    draw.line(
        [(_PADDING, divider_y), (_CARD_WIDTH - _PADDING, divider_y)],
        fill=_DIVIDER, width=1,
    )

    # ---- 正文 ----
    y = header_height
    for line in wrapped:
        _draw_text(draw, (_PADDING, y), line, body_stack, _PRIMARY_TEXT)
        y += body_line_height
    y += 12

    # ---- 图片 ----
    for img in scaled_images:
        canvas.paste(img, (_PADDING, y))
        y += img.height + 12

    # ---- 页脚 ----
    if footer:
        _draw_text(draw, (_PADDING, y), footer, footer_stack, _SECONDARY_TEXT)

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


def _font_metrics_height(font) -> int:
    try:
        ascent, descent = font.getmetrics()
        return ascent + descent + 4
    except Exception:
        return 24


def _line_height(stack: _FontStack) -> int:
    return stack.line_height()


def _char_width(ch: str, stack: _FontStack) -> int:
    try:
        font = stack.pick(ch)
        if font is None:
            return 1
        return max(1, int(font.getlength(ch)))
    except Exception:
        return max(1, len(ch) * 14)


def _draw_text(draw, xy, text: str, stack: _FontStack, fill: str) -> None:
    """按字体栈逐段绘制文本：同一字体的连续字符合并为一段。

    这样韩文/日文/emoji 等字符会自动用对应回退字体渲染，不再出现方框。
    """
    if not text or stack.primary is None:
        return
    x, y = xy
    current_font = None
    run: list[str] = []
    for ch in text:
        picked = stack.pick(ch)
        if picked is not current_font and run:
            run_text = "".join(run)
            draw.text((x, y), run_text, font=current_font, fill=fill)
            try:
                x += current_font.getlength(run_text)
            except Exception:
                x += len(run_text) * 14
            run = []
        current_font = picked
        run.append(ch)
    if run:
        draw.text((x, y), "".join(run), font=current_font, fill=fill)


def _wrap_text(text: str, stack: _FontStack, max_width: int) -> list[str]:
    if not text:
        return [""]
    if max_width < 1:
        max_width = 1

    lines: list[str] = []
    current = ""
    current_width = 0
    for ch in text:
        char_width = _char_width(ch, stack)
        if char_width > max_width:
            # 单字符超宽（极端情况）：独占一行
            if current:
                lines.append(current)
                current = ""
                current_width = 0
            lines.append(ch)
            continue
        if current_width + char_width <= max_width:
            current += ch
            current_width += char_width
            continue
        lines.append(current)
        current = ch
        current_width = char_width
    if current:
        lines.append(current)
    return lines or [""]