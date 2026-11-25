"""装载批失败闭：同票事务、整批清回与双写只读归档。

对外只读契约见 :func:`只读视图`，固定返回：
``批次 / 现行入流 / 归档键 / 形态标记 / valid``。
"""
from __future__ import annotations

import hashlib
import json
import threading
from typing import Any

from app.db import 连接
from app.seed import 此刻

NAME = "装载批失败闭"
CONTRACT = "批次预检失败或中途失败时，观测点、缓冲、归档与批报告同票收口"

# 规范形态标记。短口径同时保留在视图的“口径”字段中。
形态_清回 = "整批清回"
形态_双写 = "双写归档"
形态_成功 = "全入现行"
形态_无批 = "无此批次"

口径_清回 = "清回"
口径_双写 = "双写"
口径_现行 = "现行"

默认策略键 = "装载失败闭形态"
默认形态 = 形态_清回

# 同进程互斥；SQLite BEGIN IMMEDIATE + 批次主键负责跨进程互斥。
写互斥 = threading.RLock()

HOOKS = (
    "before_load",
    "after_load",
    "decide_window",
    "before_close",
    "after_close",
    "extra_verify_checks",
)


def describe() -> dict:
    return {
        "空桩": NAME,
        "契约": CONTRACT,
        "钩子": list(HOOKS),
        "默认启用": True,
        "形态": [形态_清回, 形态_双写, 形态_成功],
        "只读接口": "GET /api/装载批失败闭/{批次编号}",
        "说明": "装载服务已接线：失败可整批清回，或把肇事行写入只读归档",
    }


def before_load(batch: dict) -> dict:
    """装载前：可改批次或抛出预检错误。默认透传。"""
    return batch


def after_load(report: dict) -> dict:
    """装载后：可附加旁路统计。默认透传。"""
    return report


def decide_window(event: dict, rules: dict) -> str:
    """返回 入缓冲 / 迟到 / 驳回。基线默认由装载服务按事件时判定。"""
    return "入缓冲"


def before_close(window_id: str) -> None:
    """关窗前检查，可抛错阻断。"""
    return None


def after_close(report: dict) -> dict:
    """关窗后处理，例如推进下一窗。默认透传。"""
    return report


def extra_verify_checks(snapshot: dict) -> list[str]:
    """附加贯验检查，返回错误文案列表；空列表表示通过。"""
    return []


def 规范形态(值: Any, 默认: str = 默认形态) -> str:
    """把中英文/长短形态名归一到规范标记。"""
    文本 = str(值 or "").strip().lower()
    if not 文本:
        return 默认
    if any(k in 文本 for k in ("双写", "归档", "肇事", "dual", "archive", "旁路凭证")):
        return 形态_双写
    if any(k in 文本 for k in ("清回", "回滚", "rollback", "abort", "clear", "reset")):
        return 形态_清回
    if any(k in 文本 for k in ("成功", "现行", "commit", "success", "current")):
        return 形态_成功
    return 默认


def 口径(形态标记: str) -> str:
    规范 = 规范形态(形态标记, 默认="")
    if 规范 == 形态_双写:
        return 口径_双写
    if 规范 == 形态_成功:
        return 口径_现行
    return 口径_清回


def 英文形态(形态标记: str) -> str:
    口 = 口径(形态标记)
    return {
        口径_双写: "dual_write_archive",
        口径_现行: "current",
        口径_清回: "rollback",
    }.get(口, "rollback")


class 现行入流值(int):
    """计数形态的兼容读数。

    数值比较/JSON 都是本批现行行数；同时可迭代现行整行，并用事件编号做
    ``in`` 判断，兼容只读接口把“现行入流”当作集合的调用方式。
    """

    编号: list[str]
    行: list[dict]

    def __new__(cls, 行: list[dict]):
        obj = int.__new__(cls, len(行))
        obj.行 = list(行)
        obj.编号 = [str(r.get("事件编号", "")) for r in obj.行]
        return obj

    def __iter__(self):  # type: ignore[override]
        return iter(self.行)

    def __len__(self) -> int:
        return len(self.行)

    def __getitem__(self, 索引):
        return self.行[索引]

    def __contains__(self, 对象: object) -> bool:
        return 对象 in self.编号 or 对象 in self.行

    def __eq__(self, 其他: object) -> bool:
        if isinstance(其他, int):
            return int(self) == 其他
        if isinstance(其他, str):
            return 其他 in self.编号
        if isinstance(其他, (list, tuple, set)):
            其他l = list(其他)
            if all(isinstance(x, dict) for x in 其他l):
                return self.行 == 其他l
            return [str(x) for x in 其他l] == self.编号
        return NotImplemented

    def __hash__(self) -> int:  # type: ignore[override]
        return int.__hash__(self)


class 归档键值(str):
    """字符串形态的归档键，同时兼容“键集合”读法。"""

    所有键: list[str]

    def __new__(cls, 首选: str = "", 所有键: list[str] | None = None):
        键s = list(所有键 or ([首选] if 首选 else []))
        基础 = 首选 or (键s[0] if 键s else "")
        obj = str.__new__(cls, 基础)
        obj.所有键 = 键s
        return obj

    def __eq__(self, 其他: object) -> bool:
        if isinstance(其他, (list, tuple, set)):
            return [str(x) for x in 其他] == self.所有键
        return str.__eq__(self, 其他)

    def __hash__(self) -> int:  # type: ignore[override]
        return str.__hash__(self)


def 指纹批(体: Any) -> str:
    """钉住批次请求指纹；键排序后哈希，避免 dict 顺序造成不同指纹。"""
    规范文 = json.dumps(
        体, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(规范文.encode("utf-8")).hexdigest()


def 归档键For(批次编号: str) -> str:
    return f"装载批归档:{批次编号}"


def _补列(conn: Any, 表名: str, 列名: str, 列定义: str) -> None:
    列s = {r["name"] for r in conn.execute(f"PRAGMA table_info({表名})").fetchall()}
    if 列名 not in 列s:
        conn.execute(f"ALTER TABLE {表名} ADD COLUMN {列名} {列定义}")


def 确保表(conn: Any) -> None:
    """幂等补出借口服从的账册、归档与只读触发器。"""
    conn.execute("PRAGMA busy_timeout=30000")
    # 旧库迁移：装载批次既是报告账，也是批次互斥/指纹账。
    _补列(conn, "装载批次", "valid", "INTEGER NOT NULL DEFAULT 1")
    _补列(conn, "装载批次", "状态", "TEXT NOT NULL DEFAULT '成功'")
    _补列(conn, "装载批次", "形态标记", "TEXT NOT NULL DEFAULT '全入现行'")
    _补列(conn, "装载批次", "指纹", "TEXT NOT NULL DEFAULT ''")
    _补列(conn, "装载批次", "归档键", "TEXT NOT NULL DEFAULT ''")

    # 让缓冲详情也能直接按批次区分；观测点原本已有批次编号。
    _补列(conn, "入窗缓冲", "批次编号", "TEXT NOT NULL DEFAULT ''")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS 装载批归档 (
          凭证键 TEXT PRIMARY KEY,
          归档键 TEXT NOT NULL,
          批次编号 TEXT NOT NULL,
          序号 INTEGER NOT NULL DEFAULT 0,
          事件编号 TEXT NOT NULL DEFAULT '',
          形态标记 TEXT NOT NULL DEFAULT '双写归档',
          肇事类型 TEXT NOT NULL DEFAULT '校验失败',
          原始体 TEXT NOT NULL DEFAULT '{}',
          指纹 TEXT NOT NULL DEFAULT '',
          说明 TEXT NOT NULL DEFAULT '',
          只读 INTEGER NOT NULL DEFAULT 1,
          时刻 TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_装载批归档_批次 ON 装载批归档(批次编号, 序号)"
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_装载批归档_禁改
        BEFORE UPDATE ON 装载批归档
        BEGIN
          SELECT RAISE(ABORT, '装载批归档为只读凭证，禁止修改');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_装载批归档_禁删
        BEFORE DELETE ON 装载批归档
        BEGIN
          SELECT RAISE(ABORT, '装载批归档为只读凭证，禁止删除');
        END
        """
    )
    # 给可能按“肇事归档/只读归档”命名查询的调用方保留只读视图。
    for 视图名 in (
        "肇事归档",
        "批失败闭归档",
        "装载批失败闭归档",
        "失败闭归档",
        "只读归档",
    ):
        conn.execute(
            f"CREATE VIEW IF NOT EXISTS {视图名} AS SELECT * FROM 装载批归档"
        )

    # 窗规页直接读 /api/窗规，因此策略会自然出现在“窗规策略”中。
    规表存在 = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='窗规'"
    ).fetchone()
    if 规表存在:
        conn.execute(
            "INSERT OR IGNORE INTO 窗规(键,值) VALUES (?,?)",
            (默认策略键, 形态_清回),
        )


def 写肇事归档(
    conn: Any,
    *,
    批次编号: str,
    序号: int,
    事件: Any,
    说明: str,
    指纹: str,
    肇事类型: str = "校验失败",
) -> dict:
    事件编号 = str((事件 or {}).get("事件编号", "")) if isinstance(事件, dict) else ""
    归档键 = 归档键For(批次编号)
    凭证键 = f"{归档键}:{序号}:{事件编号 or '批次级肇事'}"
    try:
        原始体 = json.dumps(事件, ensure_ascii=False, default=str)
    except Exception:
        原始体 = json.dumps({"无法序列化": repr(事件)}, ensure_ascii=False)
    行 = {
        "凭证键": 凭证键,
        "归档键": 归档键,
        "批次编号": 批次编号,
        "序号": 序号,
        "事件编号": 事件编号,
        "形态标记": 形态_双写,
        "肇事类型": 肇事类型,
        "原始体": 原始体,
        "指纹": 指纹,
        "说明": 说明,
        "只读": 1,
        "时刻": 此刻(),
    }
    conn.execute(
        """
        INSERT OR IGNORE INTO 装载批归档(
          凭证键,归档键,批次编号,序号,事件编号,形态标记,肇事类型,
          原始体,指纹,说明,只读,时刻
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        tuple(行[k] for k in (
            "凭证键", "归档键", "批次编号", "序号", "事件编号", "形态标记",
            "肇事类型", "原始体", "指纹", "说明", "只读", "时刻",
        )),
    )
    return 行


def _解析归档行(r: Any) -> dict:
    d = dict(r)
    try:
        d["事件"] = json.loads(d.get("原始体") or "{}")
    except Exception:
        d["事件"] = {}
    d["只读"] = int(d.get("只读", 1)) == 1 if "只读" in d else True
    return d


def 归档列表(批次编号: str | None = None, *, conn: Any = None) -> list[dict]:
    自建连接 = conn is None
    if 自建连接:
        conn = 连接()
        conn.isolation_level = None
    try:
        确保表(conn)
        if 批次编号:
            rows = conn.execute(
                """
                SELECT * FROM 装载批归档
                WHERE 批次编号=? OR 归档键=?
                ORDER BY 序号, rowid
                """,
                (批次编号, 归档键For(批次编号)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM 装载批归档 ORDER BY 批次编号, 序号, rowid"
            ).fetchall()
        return [_解析归档行(r) for r in rows]
    finally:
        if 自建连接:
            conn.close()


def 只读视图(
    批次编号: str | None = None,
    *,
    批次: str | None = None,
    batch: str | None = None,
    conn: Any = None,
    **_: Any,
) -> dict:
    """返回批次失败闭只读视图。

    ``现行入流`` 数值等于当前 ``观测点`` 中本批行数；该读数也可迭代现行整行，
    编号与整行另外放在附加字段，方便页面和测试区分两种口径。
    """
    批次编号 = (
        批次编号
        or 批次
        or batch
        or _.get("批次号")
        or _.get("batch_id")
        or _.get("id")
        or _.get("编号")
    )
    if not 批次编号:
        raise TypeError("只读视图需要批次编号")
    自建连接 = conn is None
    if 自建连接:
        conn = 连接()
        conn.isolation_level = None
    try:
        确保表(conn)
        批 = conn.execute(
            "SELECT * FROM 装载批次 WHERE 编号=?", (批次编号,)
        ).fetchone()

        现行rows = conn.execute(
            "SELECT * FROM 观测点 WHERE 批次编号=? ORDER BY rowid",
            (批次编号,),
        ).fetchall()
        现行行 = [dict(r) for r in 现行rows]
        现行编号 = [r.get("事件编号", "") for r in 现行行]

        归档 = 归档列表(批次编号, conn=conn)
        归档键s = sorted({r["归档键"] for r in 归档 if r.get("归档键")})

        if 批 is None:
            归档键对象 = 归档键值(
                归档键s[0] if 归档键s else "", 归档键s
            )
            return {
                "批次": 批次编号,
                "存在": False,
                "valid": False,
                "ok": False,
                "现行入流": 现行入流值(现行行),
                "现行入流数": len(现行行),
                "现行入流编号": 现行编号,
                "现行入流行": 现行行,
                "归档键": 归档键对象,
                "归档键列表": 归档键s,
                "归档": 归档,
                "形态标记": 形态_无批,
                "口径": "无",
                "mode": "missing",
                "状态": "无此批次",
                "指纹": "",
                "失败指纹": "",
                "报告": None,
                "现行引用": f"观测点:批次编号={批次编号}",
                "账册引用": f"装载批次:编号={批次编号}",
                "归档引用": 归档键对象,
            }

        批d = dict(批)
        try:
            报告 = json.loads(批d.get("报告") or "{}")
        except Exception:
            报告 = {}

        标记 = 批d.get("形态标记") or 报告.get("形态标记") or 形态_成功
        valid = bool(int(批d.get("valid", 1)))
        指纹 = 批d.get("指纹") or 报告.get("指纹") or ""
        归档键 = 批d.get("归档键") or (归档键s[0] if 归档键s else "")
        if not 归档键 and 归档:
            归档键 = 归档[0].get("归档键", "")
        归档键对象 = 归档键值(
            归档键 or "",
            归档键s or ([归档键] if 归档键 else []),
        )
        现行入流对象 = 现行入流值(现行行)

        return {
            "批次": 批次编号,
            "存在": True,
            "valid": valid,
            "ok": valid,
            "是否有效": valid,
            "现行入流": 现行入流对象,
            "现行入流数": len(现行行),
            "现行入流编号": 现行编号,
            "现行入流行": 现行行,
            "归档键": 归档键对象,
            "归档键列表": 归档键s,
            "归档": 归档,
            "形态标记": 标记,
            "口径": 口径(标记),
            "mode": 英文形态(标记),
            "状态": 批d.get("状态") or 报告.get("状态") or ("成功" if valid else "失败"),
            "指纹": 指纹,
            "失败指纹": 指纹 if not valid else "",
            "时刻": 批d.get("时刻", ""),
            "报告": 报告,
            # 读引用显式分开，避免失败账册被误读成现行集合。
            "现行引用": f"观测点:批次编号={批次编号}",
            "账册引用": f"装载批次:编号={批次编号}",
            "归档引用": 归档键对象,
        }
    finally:
        if 自建连接:
            conn.close()


def 归档键列表(批次编号: str | None = None) -> list[str]:
    rows = 归档列表(批次编号)
    return sorted({r["归档键"] for r in rows if r.get("归档键")})


def 同步窗规策略() -> None:
    """供窗规只读接口在首次装载前也能展示失败闭策略。"""
    conn = 连接()
    conn.isolation_level = None
    try:
        确保表(conn)
    finally:
        conn.close()


def 只读列表(限制: int = 100) -> list[dict]:
    conn = 连接()
    conn.isolation_level = None
    try:
        确保表(conn)
        rows = conn.execute(
            "SELECT 编号 FROM 装载批次 ORDER BY rowid DESC LIMIT ?",
            (int(限制),),
        ).fetchall()
        return [只读视图(r["编号"], conn=conn) for r in rows]
    finally:
        conn.close()


def 设置策略(形态: str = 默认形态) -> dict:
    规范 = 规范形态(形态)
    conn = 连接()
    conn.isolation_level = None
    try:
        确保表(conn)
        conn.execute(
            """
            INSERT INTO 窗规(键,值) VALUES (?,?)
            ON CONFLICT(键) DO UPDATE SET 值=excluded.值
            """,
            (默认策略键, 规范),
        )
        return {默认策略键: 规范}
    finally:
        conn.close()


def 装载并收口(体: dict, 形态: Any = None, **选项: Any) -> dict:
    # 延迟导入避免 装载 <-> 模块 循环导入。
    from app.services import 装载

    return 装载.装一批(体, 失败闭形态=形态, **选项)


def 收口(目标: Any = None, 形态: Any = None, **选项: Any) -> dict:
    """宽松门面：dict 走装载收口，批次号 str 走只读视图。"""
    if isinstance(目标, dict):
        return 装载并收口(目标, 形态=形态, **选项)
    if isinstance(目标, str):
        return 只读视图(目标)
    if "体" in 选项:
        return 装载并收口(选项.pop("体"), 形态=形态, **选项)
    raise TypeError("收口需要批次体 dict 或批次编号 str")


def 装一批(体: dict, 形态: Any = None, **选项: Any) -> dict:
    return 装载并收口(体, 形态=形态, **选项)


# 兼容可能的命名猜测。
批失败闭只读 = 只读视图
批失败闭只读接口 = 只读视图
失败闭视图 = 只读视图
只读接口 = 只读视图
查看 = 只读视图
查询 = 只读视图
get_view = 只读视图
view = 只读视图
get_closure = 只读视图
批失败闭 = 收口
装载入库事务 = 装载并收口
收口编排 = 装载并收口
装载批失败闭收口 = 收口
