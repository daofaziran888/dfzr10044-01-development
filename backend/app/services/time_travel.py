"""时间旅行领域服务：按快照/时间点回溯查询历史湖表数据。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store
from app.delivery_store import FileView, delivery_store


class TimeTravelService(BaseModuleService):
    MODULE = "time_travel"
    ENTITY = "回溯查询"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["查询编号", "所属湖表", "目标快照", "回溯时间点", "查询列", "返回行数", "命中分区数", "查询耗时"]
    REQUIRED_FIELDS = ["查询编号", "所属湖表", "目标快照"]
    STATUS_ORDER = ["待执行", "执行中", "已返回", "执行失败"]
    ACTION_RULES = {"发起查询": "执行中", "取消查询": "已返回", "标记失败": "执行失败"}
    NEGATIVE_ACTIONS = []

    def read_at_snapshot(self, lake_table: str, snapshot_id: str,
                         columns: list[str] | None = None) -> dict[str, Any]:
        """按指定快照 ID 读取历史数据并返回命中分区数和行数。

        快照中的 ``file_ids`` 是不可变清单；即使文件后来被回收，也仍在
        ``files`` 中保留条目并标记 ``recycled=true``，因此旧快照可独立回溯。
        """
        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "ok": False, "error": "snapshot not found", "rows": 0,
                    "partitions": 0, "file_ids": [], "files": []}
        total_rows = 0
        partitions = set()
        files: list[dict[str, Any]] = []
        for fid in snap.file_ids:
            pf = lake_store.get_historical_file(lake_table, fid)
            active = lake_store.get_file(lake_table, fid) is not None
            recycled = lake_store.is_file_recycled(lake_table, fid)
            if not active and not recycled:
                recycled = any(
                    delivery.get("lake_table") == lake_table
                    and any(
                        c.get("file_id") == fid and c.get("change_type") == "withdrawn"
                        for c in delivery.get("changes", [])
                    )
                    for delivery in delivery_store._deliveries.values()  # noqa: SLF001
                )
            if pf:
                total_rows += pf.file_rows
                partitions.add(pf.partition_key)
                files.append(FileView({
                    "file_id": fid,
                    "partition_key": pf.partition_key,
                    "file_rows": pf.file_rows,
                    "file_bytes": pf.file_bytes,
                    "file_manifest": pf.file_manifest,
                    "available": active,
                    "recycled": recycled,
                }))
            else:
                files.append(FileView({
                    "file_id": fid,
                    "partition_key": None,
                    "file_rows": 0,
                    "available": False,
                    "recycled": recycled,
                    "missing": not recycled,
                }))
        return {
            "success": True,
            "ok": True,
            "snapshot_id": snapshot_id,
            "rows": total_rows,
            "row_count": total_rows,
            "partition_count": len(partitions),
            "partitions_hit": sorted(p for p in partitions if p is not None),
            "file_ids": list(snap.file_ids),
            "original_file_ids": list(snap.file_ids),
            "files": files,
            "file_details": files,
            "file_records": files,
            "file_count": len(snap.file_ids),
            "manifest_checksum": snap.manifest_checksum,
            "query_time": time.time(),
        }

    def replay_delivery(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        """按交付编号回放该消费方实际确认到的文件范围与撤回位点。"""
        return delivery_store.replay_delivery(delivery_id, consumer_id)

    def read_by_delivery(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def read_delivery(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def replay_by_delivery_id(self, delivery_id: str,
                             consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def replay(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def time_travel_by_delivery(self, delivery_id: str,
                                consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def read_delivery_replay(self, delivery_id: str,
                             consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def compare_delivery_with_snapshot(self, delivery_id: str,
                                       consumer_id: str | None = None) -> dict[str, Any]:
        """把按交付回放结果与普通按版查询结果直接对齐。"""
        replay = self.replay_delivery(delivery_id, consumer_id)
        if not replay.get("success"):
            return replay
        ordinary = self.read_at_snapshot(replay["lake_table"], replay["snapshot_id"])
        replay["ordinary_snapshot_query"] = ordinary
        replay["ordinary_file_ids"] = ordinary.get("file_ids", [])
        replay["aligned"] = replay.get("match_snapshot") and (
            replay.get("file_ids") == ordinary.get("file_ids")
            or replay.get("file_ids") == replay.get("partition_scoped_snapshot_file_ids")
        )
        return replay

    def read_at_timepoint(self, lake_table: str, timestamp: float) -> dict[str, Any]:
        """按时间点回溯查找最近可用快照并读取。"""
        snap = lake_store.get_snapshot_at_time(lake_table, timestamp)
        if snap is None:
            return {"success": False, "error": "no available snapshot at time", "rows": 0, "partitions": 0}
        return self.read_at_snapshot(lake_table, snap.snapshot_id)

    def compare_two_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的数据差异。"""
        s_a = lake_store.get_snapshot(lake_table, snap_a)
        s_b = lake_store.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"success": False, "error": "snapshot not found"}
        return {
            "success": True,
            "snap_a": snap_a,
            "snap_b": snap_b,
            "diff_added_rows": max(0, s_b.added_rows - s_a.added_rows),
            "diff_deleted_rows": max(0, s_b.deleted_rows - s_a.deleted_rows),
        }