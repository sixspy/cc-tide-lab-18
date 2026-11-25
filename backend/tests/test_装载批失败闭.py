from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import copy
import os
import tempfile

import pytest

_tmp = tempfile.mkdtemp()
os.environ["数据目录"] = _tmp
os.environ["夹具目录"] = str(Path(__file__).resolve().parents[2] / "fixtures")

from app import db, seed
from app.modules.装载批失败闭 import 只读视图, 归档列表
from app.services import 目录, 装载, 关窗, 贯验, 统计


def _事件(编号: str, **覆盖) -> dict:
    行 = {
        "事件编号": 编号,
        "运单键": f"运单-{编号}",
        "网点键": "网点-东站",
        "事件时": "2026-11-24T00:15:00Z",
    }
    行.update(覆盖)
    return 行


@pytest.fixture()
def 独立库(tmp_path, monkeypatch):
    路径 = tmp_path / "装载批失败闭.db"
    monkeypatch.setattr(db, "库路径", 路径)
    seed.初始化库()
    yield 路径


def _本批观测(批次: str) -> list[dict]:
    return [r for r in 目录.观测列表() if r.get("批次编号") == 批次]


def _本批缓冲编号(编号集合: set[str]) -> list[dict]:
    return [r for r in 目录.缓冲列表() if r["事件编号"] in 编号集合]


def test_场景一_批内前成后败_清回与双写口径(独立库):
    坏行 = _事件("E-bad-2", 运单键="坏 运单 键")
    体 = {"批次编号": "B-bad", "事件": [_事件("E-ok-1"), 坏行]}

    清回报告 = 装载.装一批(copy.deepcopy(体), 失败闭形态="清回")
    assert 清回报告["valid"] is False
    assert "清回" in 清回报告["状态"]

    清回视图 = 只读视图("B-bad")
    assert set(清回视图) >= {"批次", "现行入流", "归档键", "形态标记", "valid"}
    assert 清回视图["批次"] == "B-bad"
    assert 清回视图["valid"] is False
    assert 清回视图["现行入流"] == 0
    assert 清回视图["形态标记"] in {"清回", "整批清回"}
    assert 清回视图["失败指纹"]
    assert _本批观测("B-bad") == []
    assert _本批缓冲编号({"E-ok-1", "E-bad-2"}) == []

    双写体 = {"批次编号": "B-dual", "事件": [_事件("E-dual-1"), _事件("E-dual-bad", 运单键="坏 键")]}
    双写报告 = 装载.装一批(双写体, 失败闭形态="双写")
    assert 双写报告["valid"] is False

    双写视图 = 只读视图("B-dual")
    assert 双写视图["valid"] is False
    assert 双写视图["现行入流"] == 1
    assert 双写视图["形态标记"] in {"双写", "双写归档"}
    assert "E-dual-1" in 双写视图["现行入流编号"]
    assert "E-dual-bad" not in 双写视图["现行入流编号"]
    assert 双写视图["归档键"]

    现行 = _本批观测("B-dual")
    assert [r["事件编号"] for r in 现行] == ["E-dual-1"]
    assert len(_本批缓冲编号({"E-dual-1"})) == 1
    assert _本批缓冲编号({"E-dual-bad"}) == []

    归档 = 归档列表("B-dual")
    assert 归档
    assert any(r["事件编号"] == "E-dual-bad" for r in 归档)
    assert all(r.get("只读", 1) == 1 for r in 归档)

    # 双写留下的现行前成行仍走主链，关窗/贯验不得把归档肇事行算进来。
    关双写 = 关窗.关当前窗()
    assert 关双写["缓冲消耗"] == 1
    assert 贯验.跑一次()["是否对齐"] is True


def test_场景二_纯成功全部入现行并可关窗贯验(独立库):
    体 = {
        "批次编号": "B-ok",
        "事件": [
            _事件("E-ok-a", 运单键="运单-同单"),
            _事件("E-ok-b", 运单键="运单-同单", 事件时="2026-11-24T00:35:00Z"),
        ],
    }
    报告 = 装载.装一批(体)
    assert 报告["valid"] is True
    assert 报告["入流"] == 2
    assert 报告["入缓冲"] == 2

    视图 = 只读视图("B-ok")
    assert 视图["valid"] is True
    assert 视图["现行入流"] == 2
    assert 视图["形态标记"] in {"现行", "全入现行"}
    assert not 视图["归档键"]
    assert {r["事件编号"] for r in _本批观测("B-ok")} == {"E-ok-a", "E-ok-b"}

    关 = 关窗.关当前窗()
    assert 关["成片数"] >= 1
    验 = 贯验.跑一次()
    assert 验["是否对齐"] is True


def test_场景三_同批与下一批并发不得出现无主半截现行(独立库):
    失败批 = {"批次编号": "B-race-bad", "事件": [_事件("E-race-ok"), _事件("E-race-bad", 运单键="坏 键")]}
    下一批 = {"批次编号": "B-race-next", "事件": [_事件("E-next-1"), _事件("E-next-2")]}

    def 装失败(_):
        return 装载.装一批(copy.deepcopy(失败批), 失败闭形态="清回")

    def 装下一批(_):
        return 装载.装一批(copy.deepcopy(下一批))

    with ThreadPoolExecutor(max_workers=8) as pool:
        失败回执 = list(pool.map(装失败, range(4)))
        下一批回执 = list(pool.map(装下一批, range(4)))

    assert all(r["valid"] is False for r in 失败回执)
    assert all(r["批次编号"] == "B-race-bad" for r in 失败回执)
    assert sum(1 for r in 失败回执 if r["首次执行"] is True) == 1
    assert all(r["valid"] is True for r in 下一批回执)
    assert sum(1 for r in 下一批回执 if r["首次执行"] is True) == 1

    assert _本批观测("B-race-bad") == []
    下一批行 = _本批观测("B-race-next")
    assert len(下一批行) == 2
    assert {r["事件编号"] for r in 下一批行} == {"E-next-1", "E-next-2"}

    批次行 = 统计.装载批次摘要(限制=50)
    编号 = {r["编号"] for r in 批次行}
    assert "B-race-bad" in 编号
    assert "B-race-next" in 编号


def test_只读接口与窗规策略走HTTP(独立库):
    from starlette.testclient import TestClient
    from app.main import app

    body = {
        "批次编号": "B-http",
        "失败闭形态": "双写",
        "事件": [_事件("E-http-1"), _事件("E-http-bad", 运单键="坏 键")],
    }
    with TestClient(app) as client:
        post = client.post("/api/观测点/装载", json=body)
        assert post.status_code == 200
        assert post.json()["valid"] is False

        got = client.get("/api/装载批失败闭/B-http")
        assert got.status_code == 200
        view = got.json()
        assert set(view) >= {"批次", "现行入流", "归档键", "形态标记", "valid"}
        assert view["批次"] == "B-http"
        assert view["现行入流"] == 1
        assert view["归档键"]

        query = client.get("/api/装载批失败闭只读", params={"批次": "B-http"})
        assert query.status_code == 200
        assert query.json()["批次"] == "B-http"

        观测 = client.get("/api/观测点", params={"批次编号": "B-http"}).json()
        assert [r["事件编号"] for r in 观测] == ["E-http-1"]
        缓冲 = client.get("/api/入窗缓冲", params={"批次编号": "B-http"}).json()
        assert [r["事件编号"] for r in 缓冲] == ["E-http-1"]

        概览 = client.get("/api/概览").json()
        assert 概览["装载批失败闭"]["失败批数"] >= 1

        rules = client.get("/api/窗规").json()
        assert rules.get("装载失败闭形态")
