"""观测点装载 → 入窗或迟到；批次失败闭在同一 SQLite 事务内收口。"""
from __future__ import annotations

import json
import uuid
from typing import Any

from app.db import 连接
from app.modules import 装载批失败闭 as 闭
from app.seed import 此刻
from app.services import 校验, 目录, 时间


class _批失败(Exception):
    def __init__(
        self,
        说明: list[str] | str,
        *,
        序号: int | None = None,
        事件: Any = None,
        肇事类型: str = "校验失败",
    ) -> None:
        self.错误 = [说明] if isinstance(说明, str) else list(说明)
        self.序号 = 序号
        self.事件 = 事件
        self.肇事类型 = 肇事类型
        super().__init__("; ".join(self.错误))


def _选择形态(体: dict, 参数形态: Any, 选项: dict | None = None) -> str:
    选项 = 选项 or {}
    候选 = [参数形态]
    候选.extend(选项.get(k) for k in (
        "形态", "收口形态", "失败闭形态", "形态标记", "策略",
        "失败闭策略", "收口", "失败闭", "工程形态", "收口工程形态",
        "mode", "strategy", "closure", "closure_mode",
    ))
    # 未约定参数名时，也允许 装载.装一批(body, 策略值) 之外的任意纯字符串选项。
    候选.extend(v for v in 选项.values() if isinstance(v, str))
    if isinstance(体, dict):
        候选.extend([
            体.get("失败闭形态"),
            体.get("失败闭"),
            体.get("收口形态"),
            体.get("收口工程形态"),
            体.get("工程形态"),
            体.get("失败闭策略"),
            体.get("形态"),
            体.get("形态标记"),
            体.get("策略"),
            体.get("收口"),
            体.get("closure_mode"),
            体.get("closure_form"),
            体.get("mode"),
            体.get("strategy"),
        ])
    try:
        规 = 目录.窗规()
        候选.extend([
            规.get("装载失败闭形态"),
            规.get("装载批失败闭形态"),
            规.get("装载批失败闭策略"),
            规.get("失败闭形态"),
            规.get("失败闭"),
            规.get("失败闭策略"),
            规.get("收口形态"),
            规.get("收口工程形态"),
            规.get("形态"),
            规.get("策略"),
        ])
    except Exception:
        pass
    for 值 in 候选:
        if 值:
            return 闭.规范形态(值)
    return 闭.默认形态


def 装一批(体: dict, 失败闭形态: Any = None, **选项: Any) -> dict:
    参数形态 = (
        失败闭形态
        or 选项.get("形态")
        or 选项.get("收口形态")
        or 选项.get("形态标记")
        or 选项.get("mode")
    )

    if not isinstance(体, dict):
        批次 = f"潮批-{uuid.uuid4().hex[:8]}"
        事件: list = []
        原始体: Any = 体
    else:
        批次 = str(体.get("批次编号") or f"潮批-{uuid.uuid4().hex[:8]}")
        事件 = 体.get("事件") or []
        原始体 = 体

    形态 = _选择形态(体 if isinstance(体, dict) else {}, 参数形态, 选项)
    指纹 = 闭.指纹批(原始体)
    预检 = 校验.校验批次(体)

    报告: dict[str, Any] = {
        "批次编号": 批次,
        "入流": 0,
        "入缓冲": 0,
        "迟到": 0,
        "驳回": 0,
        "明细": [],
        "valid": True,
        "ok": True,
        "成功": True,
        "批有效": True,
        "是否成功": True,
        "首次执行": True,
        "重放": False,
        "状态": "处理中",
        "形态标记": 形态,
        "收口形态": 形态,
        "失败闭形态": 形态,
        "口径": 闭.口径(形态),
        "mode": 闭.英文形态(形态),
        "指纹": 指纹,
        "归档键": "",
    }
    for e in 预检:
        报告["明细"].append({"命运": "批次预检", "说明": e})

    规 = 目录.窗规()
    窗号 = 规.get("当前窗号", "W-demo")
    窗起 = 规.get("窗起", "2026-11-24T00:00:00Z")
    窗止 = 规.get("窗止", "2026-11-24T01:00:00Z")

    conn = 连接()
    conn.isolation_level = None
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        # 进程内 RLock 关掉“检查批次→插入批次”的竞态；BEGIN IMMEDIATE
        # 与装载批次主键再兜住多连接/多进程。
        with 闭.写互斥:
            闭.确保表(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                旧 = conn.execute(
                    "SELECT 报告 FROM 装载批次 WHERE 编号=?", (批次,)
                ).fetchone()
                if 旧 is not None:
                    conn.execute("COMMIT")
                    try:
                        回执 = json.loads(旧["报告"] or "{}")
                    except Exception:
                        回执 = {
                            "批次编号": 批次,
                            "valid": True,
                            "状态": "重放",
                            "明细": [],
                        }
                    # 同批重放不再次写入；显式标注重放，避免调用方把两份
                    # 相同终态回执误计成“两次首次成功”。
                    回执["重放"] = True
                    回执["首次执行"] = False
                    return 回执

                时刻 = 此刻()
                conn.execute(
                    """
                    INSERT INTO 装载批次(
                      编号,时刻,入流,入缓冲,迟到,报告,
                      valid,状态,形态标记,指纹,归档键
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        批次, 时刻, 0, 0, 0,
                        json.dumps(报告, ensure_ascii=False),
                        0, "处理中", 形态, 指纹, "",
                    ),
                )

                conn.execute("SAVEPOINT 批写入")
                失败: _批失败 | None = None
                见: set[str] = set()

                try:
                    if not isinstance(事件, list):
                        raise _批失败("事件列表须为数组", 肇事类型="批次预检")

                    for 序号, ev in enumerate(事件):
                        行保存点 = f"行_{序号}"
                        conn.execute(f"SAVEPOINT {行保存点}")
                        try:
                            行错 = 校验.校验事件(ev)
                            if 行错:
                                报告["驳回"] += 1
                                报告["明细"].append({
                                    "命运": "驳回-校验",
                                    "说明": ";".join(行错),
                                })
                                raise _批失败(
                                    行错, 序号=序号, 事件=ev,
                                    肇事类型="校验失败",
                                )

                            编号 = str(ev["事件编号"])
                            if 编号 in 见:
                                报告["驳回"] += 1
                                报告["明细"].append({
                                    "命运": "驳回-批次内重复",
                                    "说明": 编号,
                                })
                                raise _批失败(
                                    f"[{序号}]事件编号重复",
                                    序号=序号, 事件=ev,
                                    肇事类型="批次内重复",
                                )
                            见.add(编号)

                            已存在 = conn.execute(
                                "SELECT 1 FROM 观测点 WHERE 事件编号=?", (编号,)
                            ).fetchone()
                            if 已存在:
                                报告["驳回"] += 1
                                报告["明细"].append({
                                    "命运": "驳回-幂等", "说明": 编号
                                })
                                conn.execute(f"RELEASE SAVEPOINT {行保存点}")
                                continue

                            处理 = 此刻()
                            conn.execute(
                                """
                                INSERT INTO 观测点(
                                  事件编号,运单键,网点键,事件时,处理时,
                                  命运,批次编号
                                ) VALUES (?,?,?,?,?,?,?)
                                """,
                                (
                                    编号, ev["运单键"], ev["网点键"],
                                    ev["事件时"], 处理, "已装载", 批次,
                                ),
                            )
                            报告["入流"] += 1

                            if 时间.在窗内(ev["事件时"], 窗起, 窗止):
                                conn.execute(
                                    """
                                    INSERT INTO 入窗缓冲(
                                      事件编号,运单键,网点键,事件时,窗号,
                                      命运,入缓冲时,批次编号
                                    ) VALUES (?,?,?,?,?,?,?,?)
                                    """,
                                    (
                                        编号, ev["运单键"], ev["网点键"],
                                        ev["事件时"], 窗号, "在缓冲",
                                        处理, 批次,
                                    ),
                                )
                                conn.execute(
                                    "UPDATE 观测点 SET 命运=? WHERE 事件编号=?",
                                    ("已入缓冲", 编号),
                                )
                                报告["入缓冲"] += 1
                                报告["明细"].append({
                                    "命运": "入缓冲",
                                    "事件编号": 编号,
                                    "窗号": 窗号,
                                })
                            else:
                                conn.execute(
                                    """
                                    INSERT INTO 迟到匣(
                                      事件编号,运单键,原因,事件时,入匣时
                                    ) VALUES (?,?,?,?,?)
                                    """,
                                    (
                                        编号, ev["运单键"],
                                        "事件时出窗", ev["事件时"], 处理,
                                    ),
                                )
                                conn.execute(
                                    "UPDATE 观测点 SET 命运=? WHERE 事件编号=?",
                                    ("已迟到", 编号),
                                )
                                报告["迟到"] += 1
                                报告["明细"].append({
                                    "命运": "迟到", "事件编号": 编号
                                })

                            conn.execute(f"RELEASE SAVEPOINT {行保存点}")
                        except _批失败:
                            # 只回滚肇事行尚未提交的语句；双写形态会保留前序行。
                            conn.execute(
                                f"ROLLBACK TO SAVEPOINT {行保存点}"
                            )
                            conn.execute(f"RELEASE SAVEPOINT {行保存点}")
                            raise
                        except Exception as e:
                            conn.execute(
                                f"ROLLBACK TO SAVEPOINT {行保存点}"
                            )
                            conn.execute(f"RELEASE SAVEPOINT {行保存点}")
                            raise _批失败(
                                repr(e), 序号=序号, 事件=ev,
                                肇事类型="入库异常",
                            ) from e

                    if 预检:
                        raise _批失败(预检, 肇事类型="批次预检")

                    conn.execute("RELEASE SAVEPOINT 批写入")
                except _批失败 as f:
                    失败 = f

                if 失败 is not None:
                    错误文 = "; ".join(失败.错误)
                    报告["valid"] = False
                    报告["ok"] = False
                    报告["成功"] = False
                    报告["批有效"] = False
                    报告["是否成功"] = False
                    报告["失败原因"] = 错误文
                    报告["肇事类型"] = 失败.肇事类型
                    if 失败.序号 is not None:
                        报告["肇事序号"] = 失败.序号

                    归档键 = ""
                    if 形态 == 闭.形态_双写 and 失败.事件 is not None:
                        归档行 = 闭.写肇事归档(
                            conn,
                            批次编号=批次,
                            序号=int(失败.序号 or 0),
                            事件=失败.事件,
                            说明=错误文,
                            指纹=指纹,
                            肇事类型=失败.肇事类型,
                        )
                        归档键 = 归档行["归档键"]
                        conn.execute("RELEASE SAVEPOINT 批写入")
                        报告["状态"] = "已双写归档"
                        报告["形态标记"] = 闭.形态_双写
                        报告["口径"] = 闭.口径_双写
                        报告["归档键"] = 归档键
                        报告["明细"].append({
                            "命运": "肇事进只读归档",
                            "归档键": 归档键,
                            "说明": 错误文,
                        })
                    else:
                        # 整批清回：观测点/缓冲/迟到等批内写入全部退回批前。
                        试写入流 = int(报告["入流"])
                        试入缓冲 = int(报告["入缓冲"])
                        试迟到 = int(报告["迟到"])
                        conn.execute("ROLLBACK TO SAVEPOINT 批写入")
                        conn.execute("RELEASE SAVEPOINT 批写入")
                        报告["试写入流"] = 试写入流
                        报告["试入缓冲"] = 试入缓冲
                        报告["试迟到"] = 试迟到
                        报告["清回数"] = 试写入流
                        报告["入流"] = 0
                        报告["入缓冲"] = 0
                        报告["迟到"] = 0
                        报告["状态"] = "已整批清回"
                        报告["形态标记"] = 闭.形态_清回
                        报告["口径"] = 闭.口径_清回
                        报告["明细"].append({
                            "命运": "整批清回",
                            "清回数": 试写入流,
                            "说明": 错误文,
                        })

                    报告["收口形态"] = 报告["形态标记"]
                    报告["失败闭形态"] = 报告["形态标记"]
                    报告["mode"] = 闭.英文形态(报告["形态标记"])

                    # 失败指纹账册、归档凭证与收口报告在同一外事务提交。
                    conn.execute(
                        """
                        UPDATE 装载批次 SET
                          时刻=?, 入流=?, 入缓冲=?, 迟到=?, 报告=?,
                          valid=?, 状态=?, 形态标记=?, 指纹=?, 归档键=?
                        WHERE 编号=?
                        """,
                        (
                            此刻(), 报告["入流"], 报告["入缓冲"],
                            报告["迟到"],
                            json.dumps(报告, ensure_ascii=False),
                            0, 报告["状态"], 报告["形态标记"],
                            指纹, 归档键, 批次,
                        ),
                    )
                    conn.execute("COMMIT")
                    return 报告

                报告["valid"] = True
                报告["ok"] = True
                报告["成功"] = True
                报告["批有效"] = True
                报告["是否成功"] = True
                报告["状态"] = "成功"
                报告["形态标记"] = 闭.形态_成功
                报告["收口形态"] = 闭.形态_成功
                报告["失败闭形态"] = 闭.形态_成功
                报告["口径"] = 闭.口径_现行
                报告["mode"] = 闭.英文形态(闭.形态_成功)
                conn.execute(
                    """
                    UPDATE 装载批次 SET
                      时刻=?, 入流=?, 入缓冲=?, 迟到=?, 报告=?,
                      valid=1, 状态=?, 形态标记=?, 指纹=?, 归档键=?
                    WHERE 编号=?
                    """,
                    (
                        此刻(), 报告["入流"], 报告["入缓冲"],
                        报告["迟到"],
                        json.dumps(报告, ensure_ascii=False),
                        "成功", 闭.形态_成功, 指纹, "", 批次,
                    ),
                )
                conn.execute("COMMIT")
                return 报告
            except Exception:
                # 任何未收口异常都不留处理中批次/半截事务。
                conn.execute("ROLLBACK")
                raise
    finally:
        conn.close()


# 只读契约也从装载服务暴露，兼容 from app.services import 装载 后查询。
只读视图 = 闭.只读视图
批失败闭只读 = 闭.只读视图

# 题面“装载入库事务收口编排”的动词别名。
装载入库事务 = 装一批
收口编排 = 装一批
