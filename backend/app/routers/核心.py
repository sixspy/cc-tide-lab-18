from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from pathlib import Path
from app.services import 目录, 装载, 关窗, 贯验, 夹具, 统计, 说明
from app.modules.装载批失败闭 import 只读视图, 只读列表, 同步窗规策略


async def api_概览(request: Request):
    概览 = 目录.概览()
    try:
        闭列表 = 只读列表(20)
        概览["装载批失败闭"] = {
            "失败批数": sum(1 for x in 闭列表 if not x.get("valid")),
            "最近": 闭列表[:5],
        }
    except Exception:
        # 概览主链不得因扩展账册迁移问题而白屏。
        pass
    return JSONResponse(概览)


async def api_仓情(request: Request):
    return JSONResponse(说明.仓情())


async def api_窗规_get(request: Request):
    同步窗规策略()
    return JSONResponse(目录.窗规())


async def api_窗规_post(request: Request):
    body = await request.json()
    return JSONResponse(目录.改窗规(body))


async def api_网点(request: Request):
    return JSONResponse(目录.网点列表())


def _查询批次(request: Request) -> str:
    q = request.query_params
    return q.get("批次编号") or q.get("批次") or q.get("batch") or q.get("batch_id") or ""


async def api_观测(request: Request):
    rows = 目录.观测列表()
    批次 = _查询批次(request)
    if 批次:
        rows = [r for r in rows if str(r.get("批次编号") or "") == 批次]
    return JSONResponse(rows)


async def api_缓冲(request: Request):
    rows = 目录.缓冲列表()
    批次 = _查询批次(request)
    if 批次:
        rows = [r for r in rows if str(r.get("批次编号") or "") == 批次]
    return JSONResponse(rows)


async def api_切片(request: Request):
    return JSONResponse(目录.切片列表())


async def api_迟到(request: Request):
    return JSONResponse(目录.迟到列表())


async def api_贯验列表(request: Request):
    return JSONResponse(目录.贯验列表())


async def api_夹具(request: Request):
    return JSONResponse(夹具.列表())


async def api_装载(request: Request):
    body = await request.json()
    return JSONResponse(装载.装一批(body))


async def api_夹具装载(request: Request):
    名 = request.path_params["name"]
    try:
        return JSONResponse(装载.装一批(夹具.读取(名)))
    except FileNotFoundError:
        return JSONResponse({"错误": 名}, status_code=404)


async def api_关窗(request: Request):
    return JSONResponse(关窗.关当前窗())


async def api_贯验(request: Request):
    return JSONResponse(贯验.跑一次())


async def api_统计观测(request: Request):
    return JSONResponse(统计.观测命运())


async def api_统计缓冲(request: Request):
    return JSONResponse(统计.缓冲命运())


async def api_空桩(request: Request):
    root = Path(__file__).resolve().parents[1] / "modules"
    out = []
    if root.is_dir():
        for p in sorted(root.iterdir()):
            if p.is_dir() and p.name != "__pycache__" and (p / "__init__.py").is_file():
                out.append(p.name)
    return JSONResponse({"空桩": out, "条数": len(out)})


async def api_装载批失败闭(request: Request):
    批次 = (
        request.path_params.get("batch")
        or request.path_params.get("批次")
        or request.query_params.get("批次")
        or request.query_params.get("批次编号")
        or request.query_params.get("batch")
        or request.query_params.get("batch_id")
    )
    if not 批次:
        items = 只读列表()
        return JSONResponse({"items": items, "条数": len(items)})
    视图 = 只读视图(批次)
    if not 视图.get("存在"):
        return JSONResponse(视图, status_code=404)
    return JSONResponse(视图)


路由表 = [
    Route("/api/概览", api_概览),
    Route("/api/仓情", api_仓情),
    Route("/api/窗规", api_窗规_get, methods=["GET"]),
    Route("/api/窗规", api_窗规_post, methods=["POST"]),
    Route("/api/网点源", api_网点),
    Route("/api/观测点", api_观测),
    Route("/api/入窗缓冲", api_缓冲),
    Route("/api/窗切片账", api_切片),
    Route("/api/迟到匣", api_迟到),
    Route("/api/窗贯验", api_贯验列表),
    Route("/api/夹具", api_夹具),
    Route("/api/观测点/装载", api_装载, methods=["POST"]),
    Route("/api/观测点/夹具/{name}", api_夹具装载, methods=["POST"]),
    Route("/api/关窗/跑", api_关窗, methods=["POST"]),
    Route("/api/窗贯验/跑", api_贯验, methods=["POST"]),
    Route("/api/统计/观测命运", api_统计观测),
    Route("/api/统计/缓冲命运", api_统计缓冲),
    Route("/api/装载批失败闭", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭只读", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭只读接口", api_装载批失败闭, methods=["GET"]),
    Route("/api/批失败闭只读", api_装载批失败闭, methods=["GET"]),
    Route("/api/批失败闭只读接口", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭/只读/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭/只读接口/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭只读/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/装载批失败闭只读接口/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/批失败闭只读/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/批失败闭只读接口/{batch}", api_装载批失败闭, methods=["GET"]),
    Route("/api/空桩", api_空桩),
]
