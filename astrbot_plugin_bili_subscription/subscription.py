"""订阅配置解析与推送状态存储。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path

from astrbot.api import logger


VALID_TYPES = frozenset({"video", "dynamic", "article"})
_TYPE_ALIASES = {
    "video": "video", "视频": "video",
    "dynamic": "dynamic", "动态": "dynamic",
    "article": "article", "专栏": "article", "文章": "article",
}


@dataclass(frozen=True, slots=True)
class Subscription:
    uid: str
    sessions: tuple[str, ...]
    types: frozenset[str]
    interval_minutes: int = 15


def parse_subscriptions(
    raw: object,
    *,
    default_adapter: str = "default",
    default_types: str = "video,dynamic,article",
    default_interval_minutes: int = 15,
) -> tuple[list[Subscription], list[str]]:
    """解析订阅配置。支持模板列表、JSON 数组、纯文本行三种输入。"""
    fallback_types = _parse_types(default_types) or frozenset(VALID_TYPES)
    fallback_interval = max(1, int(default_interval_minutes or 15))

    if raw is None:
        return [], []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return [], []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                return [], [f"订阅配置不是合法 JSON：{exc}"]
            if not isinstance(parsed, list):
                return [], ["订阅配置必须是数组"]
            return _parse_dict_list(
                parsed, default_adapter, fallback_types, fallback_interval
            )
        return _parse_line_format(
            text, default_adapter, fallback_types, fallback_interval
        )
    if isinstance(raw, list):
        return _parse_dict_list(
            raw, default_adapter, fallback_types, fallback_interval
        )
    return [], ["订阅配置格式不支持"]


def _parse_dict_list(
    items: list, default_adapter: str,
    fallback_types: frozenset[str], fallback_interval: int,
) -> tuple[list[Subscription], list[str]]:
    errors: list[str] = []
    by_uid: dict[str, dict[str, object]] = {}

    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            errors.append(f"第 {index} 条订阅不是对象")
            continue
        uid = str(item.get("uid") or "").strip()
        group_id = str(item.get("group_id") or "").strip()

        if not uid and not group_id:
            continue  # 跳过空行
        if not uid.isdigit():
            errors.append(f"第 {index} 条订阅的 B站UID 无效：{uid or '（空）'}")
            continue
        if not group_id:
            errors.append(f"第 {index} 条订阅缺少群号")
            continue

        session = (
            group_id if ":" in group_id
            else f"{default_adapter}:GroupMessage:{group_id}"
        )

        types = fallback_types
        if isinstance(item.get("types"), str):
            parsed = _parse_types(item["types"])
            if parsed:
                types = parsed
        elif isinstance(item.get("types"), list):
            parsed = frozenset(
                t for t in (_normalize_type(x) for x in item["types"])
                if t in VALID_TYPES
            )
            if parsed:
                types = parsed

        try:
            interval = max(1, int(item.get("interval_minutes", fallback_interval)))
        except (TypeError, ValueError):
            interval = fallback_interval

        _merge_entry(by_uid, uid, session, types, interval)

    return _finalize(by_uid), errors


def _parse_line_format(
    text: str, default_adapter: str,
    fallback_types: frozenset[str], fallback_interval: int,
) -> tuple[list[Subscription], list[str]]:
    errors: list[str] = []
    by_uid: dict[str, dict[str, object]] = {}

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 2:
            errors.append(f"第 {line_no} 行至少需要 UID 和 群号，用 | 分隔")
            continue

        uid = parts[0]
        if not uid.isdigit():
            errors.append(f"第 {line_no} 行 UID 无效：{uid or '（空）'}")
            continue
        target = parts[1]
        if not target:
            errors.append(f"第 {line_no} 行 群号 为空")
            continue

        session = (
            target if ":" in target
            else f"{default_adapter}:GroupMessage:{target}"
        )

        types = fallback_types
        if len(parts) >= 3 and parts[2]:
            parsed = _parse_types(parts[2])
            if not parsed:
                errors.append(f"第 {line_no} 行 推送类型 无效：{parts[2]}")
                continue
            types = parsed

        interval = fallback_interval
        if len(parts) >= 4 and parts[3]:
            try:
                interval = max(1, int(parts[3]))
            except ValueError:
                errors.append(f"第 {line_no} 行 间隔分钟 无效：{parts[3]}")
                continue

        _merge_entry(by_uid, uid, session, types, interval)

    return _finalize(by_uid), errors


def _merge_entry(
    store: dict[str, dict[str, object]],
    uid: str, session: str,
    types: frozenset[str], interval: int,
) -> None:
    entry = store.setdefault(
        uid, {"sessions": set(), "types": set(), "interval": interval}
    )
    sessions_set = entry["sessions"]
    types_set = entry["types"]
    assert isinstance(sessions_set, set) and isinstance(types_set, set)
    sessions_set.add(session)
    types_set.update(types)
    entry["interval"] = min(int(entry["interval"]), interval)


def _finalize(store: dict[str, dict[str, object]]) -> list[Subscription]:
    result: list[Subscription] = []
    for uid, entry in store.items():
        sessions = entry["sessions"]
        types = entry["types"]
        assert isinstance(sessions, set) and isinstance(types, set)
        result.append(
            Subscription(
                uid=uid,
                sessions=tuple(sorted(sessions)),
                types=frozenset(types),
                interval_minutes=int(entry["interval"]),
            )
        )
    return result


def _parse_types(text: str) -> frozenset[str]:
    result: set[str] = set()
    for piece in text.replace("，", ",").split(","):
        normalized = _normalize_type(piece)
        if normalized in VALID_TYPES:
            result.add(normalized)
    return frozenset(result)


def _normalize_type(value: object) -> str:
    key = str(value or "").strip().casefold()
    return _TYPE_ALIASES.get(key, key)


class SubscriptionStateStore:
    """记录每个 (uid, type) 最近推送的 ID。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._state: dict[str, dict[str, str]] = {}
        self._lock = asyncio.Lock()
        self._loaded = False
        self._dirty = False

    async def load(self) -> None:
        async with self._lock:
            if self._loaded:
                return
            self._loaded = True
            if not self._path.exists():
                return
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("bili-subscription 状态文件读取失败：%s", exc)
                return
            if not isinstance(raw, dict):
                return
            for uid, entry in raw.items():
                if not isinstance(entry, dict):
                    continue
                # 兼容旧格式：忽略历史遗留的 *_ts 键
                self._state[str(uid)] = {
                    str(k): str(v)
                    for k, v in entry.items()
                    if not str(k).endswith("_ts")
                }

    async def get(self, uid: str, content_type: str) -> str | None:
        async with self._lock:
            entry = self._state.get(uid)
            return entry.get(content_type) if entry else None

    async def set(self, uid: str, content_type: str, item_id: str) -> None:
        async with self._lock:
            self._state.setdefault(uid, {})[content_type] = item_id
            self._dirty = True

    async def save(self) -> None:
        async with self._lock:
            if not self._dirty:
                return
            text = json.dumps(self._state, ensure_ascii=False, indent=2)
            self._dirty = False
        self._path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._path.write_text, text, encoding="utf-8")