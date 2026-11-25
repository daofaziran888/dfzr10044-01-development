"""交付视图与消费视图刷新流程的专用 HTTP 接口。"""
from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.delivery_store import delivery_store
from app.lake_store import PartitionFile, lake_store
from app.store import store
from app.services.delivery_view import DeliveryViewService
from app.services.publish_view import PublishViewService
from app.services.time_travel import TimeTravelService

router = APIRouter(tags=["交付视图"])
publish_service = PublishViewService()
delivery_view_service = DeliveryViewService()
time_travel_service = TimeTravelService()


def _response(result: dict[str, Any]) -> dict[str, Any] | JSONResponse:
    if result.get("success") is False or result.get("ok") is False:
        status_code = int(result.get("status_code") or 400)
        return JSONResponse(status_code=status_code, content=result)
    return result


@router.post("/api/delivery-view/scenario")
@router.post("/api/delivery_view/scenario")
@router.post("/api/publish_view/delivery-scenario")
async def seed_delivery_scenario(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    """构造题述 A/B→A/C 冲突场景，便于界面联调和验收。"""
    data = payload or {}
    suffix = uuid4().hex[:8]
    table = str(data.get("lake_table") or f"delivery_demo_{suffix}")
    consumer = str(data.get("consumer_id") or f"demo_consumer_{suffix}")
    other = f"{consumer}_other_partition"
    partitions = data.get("partitions") or ["p1"]

    for fid, partition in (("A", "p1"), ("B", "p1"), ("C", "p1"), ("P2", "p2")):
        lake_store._files.setdefault(table, {})[fid] = PartitionFile(  # noqa: SLF001
            fid, table, partition, 10, 100, f"manifest-{fid}", time.time()
        )
    s0 = lake_store.create_snapshot(table, [], None)
    s1 = lake_store.create_snapshot(table, ["A", "B", "P2"], s0.snapshot_id)
    s2 = lake_store.create_snapshot(table, ["A", "C", "P2"], s1.snapshot_id)
    for snap in (s0, s1, s2):
        lake_store.mark_snapshot_available(table, snap.snapshot_id)

    d1 = delivery_store.create_delivery(
        table, consumer, s1.snapshot_id, s0.snapshot_id, partitions=partitions,
    )
    ack_a = None
    token_b = None
    if data.get("ack_a", True):
        change_a = next(c for c in d1["changes"] if c["file_id"] == "A")
        ack_a = delivery_store.acknowledge(
            delivery_id=d1["delivery_id"], consumer_id=consumer,
            change_id=change_a["change_id"], page_id=change_a["page_id"],
            page_token=change_a["page_token"],
        )
        token_b = next(c for c in d1["changes"] if c["file_id"] == "B")["page_token"]

    d2 = None
    if str(data.get("stage") or "s2").lower() in {"s2", "2", "conflict"} or data.get("with_s2", True):
        d2 = delivery_store.create_delivery(
            table, consumer, s2.snapshot_id, s1.snapshot_id, partitions=partitions,
        )
        delivery_store.create_delivery(
            table, other, s1.snapshot_id, s0.snapshot_id, partitions=["p2"],
        )
        delivery_store.create_delivery(
            table, other, s2.snapshot_id, s1.snapshot_id, partitions=["p2"],
        )

    return {
        "ok": True,
        "success": True,
        "lake_table": table,
        "consumer_id": consumer,
        "other_consumer_id": other,
        "partitions": partitions,
        "s0_snapshot": s0.snapshot_id,
        "s1_snapshot": s1.snapshot_id,
        "s2_snapshot": s2.snapshot_id,
        "base_snapshot": s1.snapshot_id,
        "target_snapshot": s2.snapshot_id,
        "s1_delivery": d1,
        "s2_delivery": d2,
        "delivery": d2 or d1,
        "page_token_b": token_b,
        "ack_a": ack_a,
    }


@router.post("/api/deliveries")
@router.post("/api/delivery-view/deliveries")
@router.post("/api/delivery_view/deliveries")
@router.post("/api/delivery_views")
@router.post("/api/delivery-views")
@router.post("/api/consumer_deliveries")
@router.post("/api/consumer-deliveries")
@router.post("/api/publish_view/deliveries")
@router.post("/api/publish_view/refresh")
async def create_delivery(payload: dict[str, Any] | None = Body(default=None)) -> Any:
    data = payload or {}
    result = publish_service.refresh_with_changes(data)
    # refresh_with_changes 会走 refresh_view；若调用方直接要求交付，补齐 delivery。
    if result.get("success") and not result.get("delivery_id") and data.get("consumer_id"):
        result = publish_service.create_delivery(data)
    return _response(result)


@router.get("/api/deliveries")
@router.get("/api/delivery-view/deliveries")
@router.get("/api/delivery_view/deliveries")
@router.get("/api/delivery_views")
@router.get("/api/delivery-views")
@router.get("/api/consumer_deliveries")
@router.get("/api/consumer-deliveries")
@router.get("/api/publish_view/deliveries")
async def list_deliveries(
    consumer_id: str | None = Query(default=None),
    lake_table: str | None = Query(default=None),
) -> dict[str, Any]:
    rows = delivery_store.list_deliveries(consumer_id, lake_table)
    return {"items": rows, "deliveries": rows, "total": len(rows), "page": 1, "size": max(1, len(rows))}


@router.get("/api/delivery-view/overview")
@router.get("/api/delivery_view/overview")
@router.get("/api/deliveries/overview")
@router.get("/api/publish_view/overview")
async def delivery_overview(
    lake_table: str | None = Query(default=None),
    consumer_id: str | None = Query(default=None),
) -> dict[str, Any]:
    return delivery_store.overview(lake_table, consumer_id)


@router.get("/api/delivery-view/history")
@router.get("/api/delivery_view/history")
@router.get("/api/deliveries/history")
@router.get("/api/publish_view/history")
async def consumer_history(
    consumer_id: str = Query(...),
    lake_table: str | None = Query(default=None),
) -> dict[str, Any]:
    return _response(delivery_store.consumer_history(consumer_id, lake_table))


@router.post("/api/delivery-view/reconnect")
@router.post("/api/delivery_view/reconnect")
@router.post("/api/deliveries/reconnect")
@router.post("/api/publish_view/reconnect")
async def reconnect(payload: dict[str, Any] | None = Body(default=None)) -> Any:
    return _response(delivery_store.reconnect(**(payload or {})))


@router.post("/api/delivery-view/ack")
@router.post("/api/delivery_view/ack")
@router.post("/api/deliveries/ack")
@router.post("/api/publish_view/ack")
@router.post("/api/publish_view/confirm")
@router.post("/api/publish_view/checkpoint")
async def acknowledge(payload: dict[str, Any] | None = Body(default=None)) -> Any:
    return _response(delivery_store.acknowledge(**(payload or {})))


@router.get("/api/publish_view/changes")
async def publish_changes_query(
    delivery_id: str | None = Query(default=None),
    consumer_id: str | None = Query(default=None),
    lake_table: str | None = Query(default=None),
) -> Any:
    if delivery_id:
        return _response(delivery_store.delivery_detail(delivery_id, consumer_id))
    rows = delivery_store.list_deliveries(consumer_id, lake_table)
    return {"items": rows, "changes": [change for row in rows for change in row.get("changes", [])],
            "total": len(rows)}


@router.get("/api/publish_view/checkpoint")
async def get_checkpoint(
    consumer_id: str = Query(...),
    lake_table: str = Query(...),
    view_id: str | None = Query(default=None),
) -> dict[str, Any]:
    return delivery_store.checkpoint(lake_table, consumer_id, view_id)


def _find_publish_entry(entry_ref: str) -> dict[str, Any] | None:
    rows = store.rows("publish_view")
    if entry_ref.isdigit():
        found = store.find("publish_view", int(entry_ref))
        if found:
            return found
    return next((row for row in rows if str(row.get("视图编号")) == entry_ref), None)


@router.post("/api/publish_view/{entry_ref}/refresh")
async def refresh_publish_entry(
    entry_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> Any:
    entry = _find_publish_entry(entry_ref)
    if entry is None:
        return JSONResponse(status_code=404, content={"ok": False, "success": False, "error": "消费视图不存在"})
    data = payload or {}
    result = publish_service.refresh_view(
        str(entry.get("所属湖表") or data.get("lake_table") or ""),
        data.get("base_snapshot") or entry.get("绑定快照") or None,
        consumer_id=data.get("consumer_id") or entry.get("下游消费方"),
        partitions=data.get("partitions"),
        view_id=str(entry.get("视图编号") or entry_ref),
        target_snapshot=data.get("target_snapshot"),
    )
    if result.get("success"):
        entry["绑定快照"] = result.get("refreshed_snapshot")
        if result.get("delivery_id"):
            entry["最近交付编号"] = result["delivery_id"]
    return _response(result)


@router.get("/api/publish_view/{entry_ref}/changes")
async def publish_entry_changes(
    entry_ref: str,
    consumer_id: str | None = Query(default=None),
) -> Any:
    entry = _find_publish_entry(entry_ref)
    if entry is None:
        return JSONResponse(status_code=404, content={"ok": False, "success": False, "error": "消费视图不存在"})
    cid = consumer_id or str(entry.get("下游消费方") or "") or None
    view_id = str(entry.get("视图编号") or entry_ref)
    rows = delivery_store.list_deliveries(cid, str(entry.get("所属湖表")), view_id)
    if not rows and entry.get("refresh_result"):
        return entry["refresh_result"]
    latest = rows[-1] if rows else None
    if latest is None:
        return {"ok": True, "success": True, "changes": [], "items": [], "total": 0}
    return latest


@router.post("/api/publish_view/{entry_ref}/ack")
@router.post("/api/publish_view/{entry_ref}/acknowledge")
@router.post("/api/publish_view/{entry_ref}/confirm")
async def ack_publish_entry(
    entry_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> Any:
    entry = _find_publish_entry(entry_ref)
    data = dict(payload or {})
    if entry is None:
        detail = delivery_store.delivery_detail(entry_ref)
        if not detail.get("success"):
            return JSONResponse(status_code=404, content={"ok": False, "success": False, "error": "消费视图或交付不存在"})
        data.setdefault("delivery_id", entry_ref)
        return _response(delivery_store.acknowledge(**data))
    data.setdefault("consumer_id", entry.get("下游消费方"))
    if not data.get("delivery_id") and entry.get("最近交付编号"):
        data["delivery_id"] = entry["最近交付编号"]
    return _response(delivery_store.acknowledge(**data))


@router.get("/api/delivery_view/export")
@router.get("/api/delivery-view/export")
def export_delivery_view(request: Request) -> StreamingResponse:
    # 显式放在 /api/delivery_view/{delivery_id} 前，避免 export 被当成交付编号。
    from app.routers.resource import export_entries
    return export_entries("delivery_view", request, keyword=None, status=None)


@router.get("/api/deliveries/{delivery_id}")
@router.get("/api/delivery-view/{delivery_id}")
@router.get("/api/delivery_view/{delivery_id}")
@router.get("/api/delivery-view/deliveries/{delivery_id}")
@router.get("/api/delivery_view/deliveries/{delivery_id}")
@router.get("/api/publish_view/deliveries/{delivery_id}")
@router.get("/api/publish_view/delivery/{delivery_id}")
async def get_delivery(delivery_id: str, consumer_id: str | None = Query(default=None)) -> Any:
    detail = delivery_store.delivery_detail(delivery_id, consumer_id)
    if not detail.get("success"):
        return _response(detail)
    row = delivery_view_service._row_from_delivery(detail)  # noqa: SLF001
    # 同时保留通用资源行字段和富交付详情，兼容 /api/delivery_view/{id} 的两套契约。
    return {
        **detail,
        **row,
        "delivery_status": detail.get("status"),
        "status": row.get("status"),
        "resource": row,
        "entry": row,
    }


@router.get("/api/deliveries/{delivery_id}/changes")
@router.get("/api/deliveries/{delivery_id}/change-list")
@router.get("/api/delivery-view/{delivery_id}/changes")
@router.get("/api/delivery_view/{delivery_id}/changes")
@router.get("/api/delivery-view/deliveries/{delivery_id}/changes")
@router.get("/api/delivery_view/deliveries/{delivery_id}/changes")
@router.get("/api/publish_view/deliveries/{delivery_id}/changes")
async def delivery_changes(delivery_id: str, consumer_id: str | None = Query(default=None)) -> Any:
    detail = delivery_store.delivery_detail(delivery_id, consumer_id)
    if not detail.get("success"):
        return _response(detail)
    return {
        "success": True,
        "ok": True,
        "delivery_id": delivery_id,
        "changes": detail.get("changes", []),
        "added": detail.get("added", []),
        "withdrawn": detail.get("withdrawn", []),
        "removed": detail.get("removed", []),
    }


@router.get("/api/deliveries/{delivery_id}/pages/{page_id}")
async def fetch_page(
    delivery_id: str,
    page_id: str,
    consumer_id: str | None = Query(default=None),
    page_token: str | None = Query(default=None),
    allow_stale: bool = Query(default=False),
) -> Any:
    return _response(delivery_store.fetch_page(
        delivery_id=delivery_id, page_id=page_id, consumer_id=consumer_id,
        page_token=page_token, allow_stale=allow_stale,
    ))


@router.post("/api/deliveries/{delivery_id}/ack")
@router.post("/api/deliveries/{delivery_id}/acknowledge")
@router.post("/api/deliveries/{delivery_id}/confirm")
@router.post("/api/delivery-view/{delivery_id}/ack")
@router.post("/api/delivery_view/{delivery_id}/ack")
@router.post("/api/delivery-view/{delivery_id}/confirm")
@router.post("/api/delivery_view/{delivery_id}/confirm")
@router.post("/api/delivery-view/deliveries/{delivery_id}/ack")
@router.post("/api/delivery_view/deliveries/{delivery_id}/ack")
@router.post("/api/delivery-view/deliveries/{delivery_id}/acknowledge")
@router.post("/api/delivery_view/deliveries/{delivery_id}/acknowledge")
@router.post("/api/delivery-view/deliveries/{delivery_id}/confirm")
@router.post("/api/delivery_view/deliveries/{delivery_id}/confirm")
@router.post("/api/publish_view/deliveries/{delivery_id}/ack")
@router.post("/api/publish_view/deliveries/{delivery_id}/acknowledge")
@router.post("/api/publish_view/deliveries/{delivery_id}/confirm")
async def ack_delivery(delivery_id: str, payload: dict[str, Any] | None = Body(default=None)) -> Any:
    data = dict(payload or {})
    data.setdefault("delivery_id", delivery_id)
    return _response(delivery_store.acknowledge(**data))


@router.post("/api/deliveries/{delivery_id}/pages/{page_id}/ack")
@router.post("/api/deliveries/{delivery_id}/pages/{page_id}/confirm")
async def ack_page(
    delivery_id: str, page_id: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> Any:
    data = dict(payload or {})
    data.update({"delivery_id": delivery_id, "page_id": page_id})
    return _response(delivery_store.acknowledge(**data))


@router.get("/api/deliveries/{delivery_id}/replay")
@router.get("/api/delivery-view/{delivery_id}/replay")
@router.get("/api/delivery_view/{delivery_id}/replay")
@router.get("/api/delivery-view/deliveries/{delivery_id}/replay")
@router.get("/api/delivery_view/deliveries/{delivery_id}/replay")
@router.get("/api/time_travel/deliveries/{delivery_id}/replay")
@router.get("/api/time_travel/replay_delivery/{delivery_id}")
async def replay_delivery(delivery_id: str, consumer_id: str | None = Query(default=None)) -> Any:
    return _response(time_travel_service.compare_delivery_with_snapshot(delivery_id, consumer_id))
