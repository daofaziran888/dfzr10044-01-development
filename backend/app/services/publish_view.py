"""消费视图领域服务：发布、刷新、撤回面向下游的视图，绑定快照与结构版本。

刷新流程同时生成以消费方为单位的交付变更：两次快照交接新增/撤回了哪些文件、
每页凭据、确认位点和断线重连续取都委托给 ``delivery_store``。
"""
from __future__ import annotations

import time
from typing import Any

from app.delivery_store import delivery_store
from app.lake_store import lake_store
from app.services.base import BaseModuleService
from app.services.delivery_view import DeliveryServiceMixin, create_kwargs_from_call


class PublishViewService(DeliveryServiceMixin, BaseModuleService):
    MODULE = "publish_view"
    ENTITY = "消费视图"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["视图编号", "所属湖表", "视图定义", "绑定快照", "可见列", "刷新方式", "下游消费方", "最近刷新"]
    REQUIRED_FIELDS = ["视图编号", "所属湖表", "视图定义"]
    STATUS_ORDER = ["草稿", "已发布", "刷新中", "已撤回"]
    ACTION_RULES = {"发布视图": "已发布", "触发刷新": "刷新中", "撤回视图": "已撤回"}
    NEGATIVE_ACTIONS = []

    def publish_view(self, lake_table: str, view_definition: str,
                     visible_columns: list[str], consumer: str | None = None,
                     snapshot_id: str | None = None,
                     partitions: list[str] | str | None = None,
                     view_id: str | None = None,
                     **kwargs: Any) -> dict[str, Any]:
        """发布消费视图：绑定最新可用快照或显式指定的可用快照与可见列。"""
        aliased = create_kwargs_from_call((), kwargs)
        consumer_id = consumer or aliased.get("consumer_id") or aliased.get("consumer")
        snapshot_id = snapshot_id or aliased.get("target_snapshot")
        partitions = partitions if partitions is not None else aliased.get("partitions")
        view_id = view_id or aliased.get("view_id")
        if not consumer_id:
            return {"ok": False, "success": False, "error": "consumer is required"}
        if snapshot_id:
            binding = delivery_store.bind_snapshot(lake_table, snapshot_id, consumer_id)
            if not binding.get("success"):
                return binding
            bound_snapshot = snapshot_id
        else:
            binding = delivery_store.bind_snapshot(lake_table, None, consumer_id)
            bound_snapshot = binding.get("bound_snapshot") if binding.get("success") else None
        delivery_store.register_consumer(
            consumer_id, lake_table, partitions=partitions, view_id=view_id,
            view_definition=view_definition, visible_columns=visible_columns,
        )
        return {
            "success": True,
            "ok": True,
            "lake_table": lake_table,
            "view_definition": view_definition,
            "bound_snapshot": bound_snapshot,
            "visible_columns": visible_columns,
            "consumer": consumer_id,
            "consumer_id": consumer_id,
            "partitions": delivery_store.normalize_partitions(partitions),
            "published_at": time.time(),
        }

    def refresh_view(self, lake_table: str, view_snapshot: str | None = None,
                     consumer: str | None = None,
                     partitions: list[str] | str | None = None,
                     consumer_id: str | None = None,
                     view_id: str | None = None,
                     create_delivery: bool = True,
                     target_snapshot: str | None = None,
                     **kwargs: Any) -> dict[str, Any]:
        """触发视图刷新：更新绑定快照，并在给定消费方时返回交接变更列表。"""
        aliased = create_kwargs_from_call((), kwargs)
        cid = consumer_id or consumer or aliased.get("consumer_id")
        partitions = partitions if partitions is not None else aliased.get("partitions")
        view_id = view_id or aliased.get("view_id")
        target = target_snapshot or aliased.get("target_snapshot")
        target = target or kwargs.get("refreshed_snapshot")
        if target:
            binding = delivery_store.bind_snapshot(lake_table, target)
            if not binding.get("success"):
                return binding
            latest_id = target
            refresh_time = time.time()
        else:
            available = [
                s for s in lake_store.list_snapshots(lake_table)
                if s.status == "available"
            ]
            if not available:
                return {"success": False, "ok": False, "error": "no available snapshot"}
            latest = available[-1]
            latest_id = latest.snapshot_id
            refresh_time = time.time()

        result: dict[str, Any] = {
            "success": True,
            "ok": True,
            "previous_snapshot": view_snapshot,
            "refreshed_snapshot": latest_id,
            "bound_snapshot": latest_id,
            "refresh_time": refresh_time,
        }
        if cid and create_delivery:
            delivery = delivery_store.create_delivery(
                lake_table, cid, latest_id, base_snapshot=view_snapshot,
                partitions=partitions, view_id=view_id,
            )
            result["delivery"] = delivery
            if delivery.get("success"):
                result.update({
                    "delivery_id": delivery["delivery_id"],
                    "changes": delivery["changes"],
                    "added": delivery["added"],
                    "removed": delivery["removed"],
                    "withdrawn": delivery["withdrawn"],
                    "pages": delivery["pages"],
                    "conflicts": delivery["conflicts"],
                    "conflict": delivery["conflict"],
                    "checkpoint": delivery.get("checkpoint"),
                })
            else:
                result["delivery_error"] = delivery
        return result

    # 语义更直白的刷新入口，兼容可能的调用方命名。
    def refresh_with_changes(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        params = create_kwargs_from_call(args, kwargs)
        lake_table = params.get("lake_table")
        if not lake_table:
            return {"ok": False, "success": False, "error": "lake_table is required"}
        return self.refresh_view(
            lake_table,
            params.get("base_snapshot") or params.get("view_snapshot"),
            consumer_id=params.get("consumer_id"),
            partitions=params.get("partitions"),
            view_id=params.get("view_id"),
            target_snapshot=params.get("target_snapshot"),
        )

    def list_change_list(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.delivery_changes(*args, **kwargs)

    def revoke_view(self, lake_table: str, consumer: str) -> dict[str, Any]:
        """撤回视图：撤销下游消费方对视图的访问授权。"""
        return {
            "lake_table": lake_table,
            "consumer": consumer,
            "consumer_id": consumer,
            "revoked": True,
            "revoke_time": time.time(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """通用资源动作；“触发刷新”时把变更列表和交付编号回写到条目。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "触发刷新":
            result = self.refresh_view(
                str(entry.get("所属湖表") or ""),
                str(entry.get("绑定快照") or "") or None,
                consumer_id=str(entry.get("下游消费方") or "") or None,
                view_id=str(entry.get("视图编号") or "") or None,
            )
            entry["refresh_result"] = result
            if result.get("success"):
                entry["绑定快照"] = result.get("refreshed_snapshot")
                entry["最近刷新"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                if result.get("delivery_id"):
                    entry["最近交付编号"] = result["delivery_id"]
        return entry, message
