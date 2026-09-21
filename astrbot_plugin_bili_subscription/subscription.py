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


def parse_types_text(text: str | None) -> frozenset[str] | None:
    """解析推送类型字符串；无有效类型时返回 None（表示用默认值）。"""
    if text is None:
        return None
    parsed = _parse_types(str(text))
    return parsed or None


def merge_subscriptions(*groups: list[Subscription]) -> list[Subscription]:
    """把多组订阅（配置订阅 + 运行时订阅）按 UID 合并，会话/类型取并集。"""
    by_uid: dict[str, dict[str, object]] = {}
    for group in groups:
        for sub in group:
            for session in sub.sessions:
                _merge_entry(by_uid, sub.uid, session, sub.types, sub.interval_minutes)
    return _finalize(by_uid)


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

        raw_interval = item.get("interval_minutes")
        if raw_interval in (None, "", 0, "0"):
            # 留空或 0 表示用全局默认
            interval = fallback_interval
        else:
            try:
                interval = max(1, int(raw_interval))
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
    """记录每个 (uid, type) 最近推送的 ID。

    写入策略：
    - 内存状态由 lock 保护；
    - save() 用"写临时文件 → 原子 rename"防止崩溃时损坏文件；
    - 用 mutation_count 计数，写入成功后才把对应版本的 dirty 清除，
      避免写入失败导致状态永久丢失，也避免 save 期间的新改动被吞掉。
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._state: dict[str, dict[str, str]] = {}
        self._lock = asyncio.Lock()
        self._loaded = False
        self._dirty = False
        self._mutation_count = 0

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
            self._mutation_count += 1

    async def save(self) -> None:
        async with self._lock:
            if not self._dirty:
                return
            text = json.dumps(self._state, ensure_ascii=False, indent=2)
            saved_count = self._mutation_count

        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            await asyncio.to_thread(self._write_atomic, text)
        except Exception as exc:
            # 保留 _dirty=True，下次 save() 会重试
            logger.warning("bili-subscription 状态文件写入失败：%s", exc)
            return

        async with self._lock:
            # 只有在写入期间没有新的 set() 时才清除 dirty
            if self._mutation_count == saved_count:
                self._dirty = False

    def _write_atomic(self, text: str) -> None:
        """原子写入：先写临时文件，再 rename 覆盖。"""
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(self._path)


class RuntimeSubStore:
    """通过聊天命令（订阅/退订）添加的运行时订阅，独立于 WebUI 配置。

    与配置订阅分开存储（本插件数据目录下的 JSON 文件），
    解析时由 merge_subscriptions 合并，避免直接改写 AstrBot 配置。
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._entries: list[Subscription] = []
        self._lock = asyncio.Lock()
        self._loaded = False

    @property
    def count(self) -> int:
        return len(self._entries)

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
                logger.warning("bili-subscription 运行时订阅文件读取失败：%s", exc)
                return
            if not isinstance(raw, dict):
                return
            items = raw.get("subscriptions")
            if not isinstance(items, list):
                return
            for item in items:
                if not isinstance(item, dict):
                    continue
                uid = str(item.get("uid") or "").strip()
                session = str(item.get("session") or "").strip()
                if not uid.isdigit() or not session:
                    continue
                raw_types = item.get("types")
                if isinstance(raw_types, list):
                    types = frozenset(
                        t for t in (_normalize_type(x) for x in raw_types)
                        if t in VALID_TYPES
                    ) or frozenset(VALID_TYPES)
                else:
                    types = parse_types_text(str(raw_types or "")) or frozenset(VALID_TYPES)
                try:
                    interval = max(1, int(item.get("interval_minutes") or 15))
                except (TypeError, ValueError):
                    interval = 15
                self._entries.append(
                    Subscription(
                        uid=uid,
                        sessions=(session,),
                        types=types,
                        interval_minutes=interval,
                    )
                )

    def find(self, uid: str) -> list[Subscription]:
        return [e for e in self._entries if e.uid == uid]

    def to_subscriptions(self) -> list[Subscription]:
        return list(self._entries)

    def upsert(self, sub: Subscription) -> None:
        """同 UID + 同会话时更新类型/间隔，否则新增。"""
        for index, entry in enumerate(self._entries):
            if entry.uid == sub.uid and entry.sessions == sub.sessions:
                self._entries[index] = sub
                return
        self._entries.append(sub)

    def remove(self, uid: str, *, session: str | None = None) -> list[Subscription]:
        """移除运行时订阅。session 为 None 时移除该 UID 的全部。"""
        removed = [
            e for e in self._entries
            if e.uid == uid and (session is None or session in e.sessions)
        ]
        self._entries = [e for e in self._entries if e not in removed]
        return removed

    async def save(self) -> None:
        payload = {
            "subscriptions": [
                {
                    "uid": e.uid,
                    "session": e.sessions[0],
                    "types": sorted(e.types),
                    "interval_minutes": e.interval_minutes,
                }
                for e in self._entries
                if e.sessions
            ]
        }
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            await asyncio.to_thread(self._write_atomic, text)
        except Exception as exc:
            logger.warning("bili-subscription 运行时订阅保存失败：%s", exc)

    def _write_atomic(self, text: str) -> None:
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(self._path)