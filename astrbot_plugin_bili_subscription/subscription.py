"""订阅配置解析与推送状态存储。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
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


_ID_LIST_SEP = "|"


def _encode_id_list(ids: list[str]) -> str:
    """把最近推送 ID 列表编码成状态文件里的一个字符串。"""
    return _ID_LIST_SEP.join(ids)


def _decode_id_list(value: str) -> list[str]:
    """解析状态文件里的"最近推送 ID"。兼容旧格式（单个纯 ID）。"""
    text = str(value or "")
    if not text:
        return []
    if _ID_LIST_SEP in text:
        return [piece for piece in text.split(_ID_LIST_SEP) if piece]
    return [text]


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

    # 每个 (uid, type) 额外记住的最近推送 ID 数量。
    # 只留一个"最后一条"会导致：两次检查之间新增条目多于抓取窗口时，
    # 窗口里找不到上一条，于是整个窗口被当成"新条目"重推一遍。
    # 这里多留几条已知 ID，让窗口内的条目能被正确识别为"已推送过"。
    RECENT_LIMIT = 40

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._state: dict[str, dict[str, str]] = {}
        self._recent: dict[str, dict[str, list[str]]] = {}
        self._lock = asyncio.Lock()
        self._loaded = False
        self._dirty = False
        self._mutation_count = 0

    @property
    def path(self) -> Path:
        """状态文件路径（便于排查"状态没落盘导致重复推送"）。"""
        return self._path

    @property
    def tracked_uids(self) -> int:
        """已记录推送进度的 UID 数量。"""
        return len(self._state)

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
                clean = {
                    str(k): str(v)
                    for k, v in entry.items()
                    if not str(k).endswith("_ts")
                }
                state: dict[str, str] = {}
                history: dict[str, list[str]] = {}
                for key, value in clean.items():
                    ids = _decode_id_list(value)
                    if not ids:
                        continue
                    # last 就是历史里的第一条（最新）；旧格式的单个纯 ID
                    # 会被 _decode_id_list 解析成长度为 1 的列表。
                    state[key] = ids[0]
                    history[key] = ids
                self._state[str(uid)] = state
                if history:
                    self._recent[str(uid)] = history

    async def get(self, uid: str, content_type: str) -> str | None:
        async with self._lock:
            entry = self._state.get(uid)
            return entry.get(content_type) if entry else None

    async def recent(self, uid: str, content_type: str) -> tuple[str, ...]:
        """该 (uid, 类型) 最近推送过的条目 ID（从新到旧，含最后一条）。

        历史列表本身就以最新在前，所以 ``recent()[0]`` 恒等于 ``get()``
        的返回值；这里再兜一次底，避免内存状态被外部改动后不一致。
        """
        async with self._lock:
            entry = self._recent.get(uid)
            items = list(entry.get(content_type) or ()) if entry else []
            last = (self._state.get(uid) or {}).get(content_type)
            if last and (not items or items[0] != last):
                items = [last, *(i for i in items if i != last)]
            return tuple(items)

    async def set(self, uid: str, content_type: str, item_id: str) -> None:
        async with self._lock:
            self._state.setdefault(uid, {})[content_type] = item_id
            self._remember_locked(uid, content_type, item_id)
            self._dirty = True
            self._mutation_count += 1

    async def remember(self, uid: str, content_type: str, item_id: str) -> None:
        """只登记"已见过"的条目，不改变内容类型的最新标记。"""
        async with self._lock:
            if not self._remember_locked(uid, content_type, item_id):
                return
            self._dirty = True
            self._mutation_count += 1

    def _remember_locked(self, uid: str, content_type: str, item_id: str) -> bool:
        """把条目 ID 记进最近历史（去重、容量受限）。返回是否发生变化。

        存储顺序为"新 → 旧"，最新一条在最前面，与 ``recent()`` 的输出一致；
        落盘时保持相同顺序，重启后无需翻转。

        注意：历史列表始终包含当前 ``last``，所以状态文件里每个
        (uid, 类型) 只需要存这一个列表即可，不会出现"last 与历史不一致"。
        """
        if not item_id:
            return False
        bucket = self._recent.setdefault(uid, {}).setdefault(content_type, [])
        if bucket and bucket[0] == item_id:
            return False
        if item_id in bucket:
            bucket.remove(item_id)
        bucket.insert(0, item_id)
        # 超出容量时丢弃最早的记录
        del bucket[self.RECENT_LIMIT:]
        return True

    async def save(self) -> None:
        async with self._lock:
            if not self._dirty:
                return
            # 历史列表天然包含 last，因此直接落盘历史即可；
            # 只为"有 last 但没有历史"的异常内存状态补一条。
            payload: dict[str, dict[str, str]] = {}
            for uid in set(self._state) | set(self._recent):
                merged: dict[str, str] = {}
                for content_type, ids in (self._recent.get(uid) or {}).items():
                    if ids:
                        merged[content_type] = _encode_id_list(ids)
                last_entry = self._state.get(uid) or {}
                for content_type, item_id in last_entry.items():
                    if content_type not in merged and item_id:
                        merged[content_type] = _encode_id_list([item_id])
                if merged:
                    payload[uid] = merged
            text = json.dumps(payload, ensure_ascii=False, indent=2)
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


# ----------------------------------------------------------------------
# 推送台账：杜绝重复推送，并保留消息 ID 以便必要时撤回
# ----------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class SentRecord:
    """一条已发出的消息。"""

    uid: str
    kind: str          # video / dynamic / article
    item_id: str       # BV 号 / 动态 ID / 专栏 ID
    session: str       # 会话 ID
    message_id: str    # 平台消息 ID（拿不到时为空）
    ts: float          # 发出时间（unix 秒）

    @property
    def dedup_key(self) -> tuple[str, str, str]:
        return (self.uid, self.kind, self.item_id)


def dedup_key(uid: str, kind: str, item_id: str) -> tuple[str, str, str]:
    return (str(uid), str(kind), str(item_id))


class PushLedger:
    """持久化"这条内容已经发给这个会话了"的台账。

    解决的问题：
    1. **进程重启后不重复推送**。原先只在内存里记"已送达会话"，
       插件重启/热重载后记忆清零，同一条动态会被再推一遍。
    2. **状态文件窗口外的条目**。原先靠"上一条 ID"判断新旧，
       两次检查之间新增条目超过抓取窗口时判断失效，整个窗口被重推。
       台账按条目 ID 精确记账，与窗口大小无关。
    3. **撤回依据**。每条消息的 message_id 都记下来，
       一旦发现同一条内容被重复发出，可以按 ID 撤回。

    容量策略：按"最近 N 条内容记录"裁剪，超过窗口的旧账本条目会被丢弃，
    避免文件无限增长；被丢弃的极旧条目理论上可能重复推送，
    但那时它早已不在抓取窗口内，不会再被扫到。
    """

    MAX_ITEMS = 800

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        # (uid, kind, item_id) -> {session: [message_id, ...]}
        self._records: dict[tuple[str, str, str], dict[str, list[str]]] = {}
        self._lock = asyncio.Lock()
        self._loaded = False
        self._dirty = False
        self._mutation_count = 0

    @property
    def path(self) -> Path:
        return self._path

    @property
    def item_count(self) -> int:
        return len(self._records)

    def message_count(self) -> int:
        return sum(
            len(ids) for sessions in self._records.values()
            for ids in sessions.values()
        )

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
                logger.warning("bili-subscription 推送台账读取失败：%s", exc)
                return
            if not isinstance(raw, dict):
                return
            for key, value in raw.items():
                record = _decode_ledger_entry(key, value)
                if record is not None:
                    self._records[record[0]] = record[1]

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    async def delivered_sessions(
        self, uid: str, kind: str, item_id: str
    ) -> frozenset[str]:
        """（内容 → 已收到该内容的会话集合）。"""
        async with self._lock:
            entry = self._records.get(dedup_key(uid, kind, item_id))
            if not entry:
                return frozenset()
            return frozenset(entry)

    async def message_ids(
        self, uid: str, kind: str, item_id: str
    ) -> tuple[str, ...]:
        """该内容已发出的全部消息 ID（用于撤回，含卡片/合并转发各条）。"""
        async with self._lock:
            entry = self._records.get(dedup_key(uid, kind, item_id))
            if not entry:
                return ()
            seen: list[str] = []
            for ids in entry.values():
                for message_id in ids:
                    if message_id and message_id not in seen:
                        seen.append(message_id)
            return tuple(seen)

    async def known_item_ids(self, uid: str, kind: str) -> frozenset[str]:
        async with self._lock:
            return frozenset(
                item_id
                for (row_uid, row_kind, item_id) in self._records
                if row_uid == uid and row_kind == kind
            )

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    async def record(
        self,
        *,
        uid: str,
        kind: str,
        item_id: str,
        session: str,
        message_id: str,
    ) -> None:
        await self.record_many(
            uid=uid,
            kind=kind,
            item_id=item_id,
            session=session,
            message_ids=(message_id,) if message_id else (),
        )

    async def record_many(
        self,
        *,
        uid: str,
        kind: str,
        item_id: str,
        session: str,
        message_ids: Iterable[str] | str,
    ) -> None:
        """记录"该内容已发给该会话"，并追加本次发出的消息 ID。

        同一条内容可能由多条消息组成（卡片 + 合并转发），全部记下来，
        撤回时才能一次清干净，不留半截重复内容。
        """
        if isinstance(message_ids, str):
            pending = [message_ids] if message_ids else []
        else:
            pending = [str(m) for m in message_ids if m]
        key = dedup_key(uid, kind, item_id)
        async with self._lock:
            entry = self._records.setdefault(key, {})
            bucket = entry.setdefault(str(session), [])
            for message_id in pending:
                if message_id not in bucket:
                    bucket.append(message_id)
            self._prune_locked()
            self._dirty = True
            self._mutation_count += 1

    async def forget(self, uid: str, kind: str, item_id: str) -> None:
        """忘掉某条内容的台账（撤回后调用，允许以后重新推送）。"""
        async with self._lock:
            if self._records.pop(dedup_key(uid, kind, item_id), None) is None:
                return
            self._dirty = True
            self._mutation_count += 1

    async def pending_sessions(
        self, uid: str, kind: str, item_id: str, sessions: tuple[str, ...]
    ) -> tuple[str, ...]:
        """该内容还没送达的会话。"""
        delivered = await self.delivered_sessions(uid, kind, item_id)
        return tuple(s for s in sessions if s not in delivered)

    async def save(self) -> None:
        async with self._lock:
            if not self._dirty:
                return
            payload = {
                _encode_ledger_key(key): dict(value)
                for key, value in self._records.items()
            }
            text = json.dumps(payload, ensure_ascii=False, indent=2)
            saved_count = self._mutation_count

        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            await asyncio.to_thread(self._write_atomic, text)
        except Exception as exc:
            logger.warning("bili-subscription 推送台账写入失败：%s", exc)
            return

        async with self._lock:
            if self._mutation_count == saved_count:
                self._dirty = False

    def _prune_locked(self) -> None:
        """超出容量时丢弃最早的一半内容记录。"""
        if len(self._records) <= self.MAX_ITEMS:
            return
        excess = len(self._records) - self.MAX_ITEMS // 2
        for key in list(self._records.keys())[:excess]:
            self._records.pop(key, None)

    def _write_atomic(self, text: str) -> None:
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(self._path)


def _encode_ledger_key(key: tuple[str, str, str]) -> str:
    return "\x1f".join(str(piece) for piece in key)


def _decode_ledger_entry(
    key: str, value: object
) -> tuple[tuple[str, str, str], dict[str, list[str]]] | None:
    """解析台账条目。兼容旧格式（session -> 单个消息 ID 字符串）。"""
    if not isinstance(key, str) or "\x1f" not in key:
        return None
    parts = key.split("\x1f")
    if len(parts) != 3:
        return None
    if not isinstance(value, dict):
        return None
    uid, kind, item_id = parts
    if not uid or not kind or not item_id:
        return None
    sessions: dict[str, list[str]] = {}
    for session, raw in value.items():
        name = str(session)
        if isinstance(raw, str):
            sessions[name] = [raw] if raw else []
        elif isinstance(raw, (list, tuple)):
            sessions[name] = [str(m) for m in raw if m]
        else:
            sessions[name] = []
    return (uid, kind, item_id), sessions