"""以消费方为单位的快照交付存储。

这里不替代湖表快照本身：快照清单仍由 ``lake_store`` 不可变保存；本存储只维护
两次快照交接之间面向某个消费方的新增/撤回明细、页凭据、确认位点和交付履历。
"""
from __future__ import annotations

import secrets
import threading
import time
from typing import Any

from app.lake_store import lake_store


Change = dict[str, Any]
Delivery = dict[str, Any]


class FileView(dict):
    """文件明细；同时允许与文件 ID 字符串直接比较，兼容两种返回契约。"""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return other in {self.get("file_id"), self.get("file")}
        return super().__eq__(other)


class DeliveryStore:
    """进程内交付状态；所有服务实例共享同一个 ``delivery_store`` 单例。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._consumers: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._deliveries: dict[str, Delivery] = {}
        self._delivery_order: list[str] = []
        self._tokens: dict[str, dict[str, Any]] = {}
        self._counter = 0

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def reset(self) -> None:
        """清空交付状态，供测试或需要确定性内存状态的场景使用。"""
        with self._lock:
            self._consumers.clear()
            self._deliveries.clear()
            self._delivery_order.clear()
            self._tokens.clear()
            self._counter = 0

    @staticmethod
    def _now() -> float:
        return time.time()

    def _next_no(self) -> int:
        self._counter += 1
        return self._counter

    @staticmethod
    def snapshot_id_of(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            return value.get("snapshot_id") or value.get("id") or value.get("快照编号")
        return getattr(value, "snapshot_id", None) or getattr(value, "id", None)

    @staticmethod
    def normalize_partitions(value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            items = [part.strip() for part in value.split(",") if part.strip()]
        elif isinstance(value, dict):
            items = []
            for key, part in value.items():
                items.extend([f"{key}={part}", str(part)])
        elif isinstance(value, (list, tuple, set)):
            items = [str(part).strip() for part in value if str(part).strip()]
        else:
            items = [str(value).strip()]
        if not items or any(item in {"*", "all", "ALL", "全部"} for item in items):
            return None
        return items

    @staticmethod
    def _partition_matches(partition_key: str | None, filters: list[str] | None) -> bool:
        if not filters:
            return True
        if not partition_key:
            # 限定分区的订阅不能因为缺少元数据而收到无法判定的数据。
            return False
        for item in filters:
            if item.endswith("*"):
                if partition_key.startswith(item[:-1]):
                    return True
            elif partition_key == item:
                return True
            elif partition_key.endswith(f"={item}") or partition_key.endswith(f"/{item}"):
                return True
            elif f"={item}/" in partition_key or f"/{item}/" in partition_key:
                return True
        return False

    def _file_info(
        self,
        lake_table: str,
        file_id: str,
        partition_overrides: dict[str, str] | None = None,
    ) -> FileView:
        override = (partition_overrides or {}).get(file_id)
        pf = lake_store.get_historical_file(lake_table, file_id)
        recycled = lake_store.is_file_recycled(lake_table, file_id)
        info = FileView({
            "file_id": file_id,
            "file": file_id,
            "partition_key": override or (pf.partition_key if pf else None),
            "file_rows": pf.file_rows if pf else None,
            "file_bytes": pf.file_bytes if pf else None,
            "file_manifest": pf.file_manifest if pf else None,
            "available": lake_store.get_file(lake_table, file_id) is not None,
            "recycled": recycled,
        })
        if pf is not None:
            info["created_at"] = pf.created_at
        return info

    def _snapshot_file_ids(self, lake_table: str, snapshot_id: str | None) -> list[str]:
        snap = lake_store.get_snapshot(lake_table, snapshot_id or "")
        return list(snap.file_ids) if snap else []

    def _visible_file_ids(
        self,
        lake_table: str,
        snapshot_id: str | None,
        partitions: list[str] | None,
        overrides: dict[str, str] | None = None,
    ) -> list[str]:
        result: list[str] = []
        for file_id in self._snapshot_file_ids(lake_table, snapshot_id):
            info = self._file_info(lake_table, file_id, overrides)
            if self._partition_matches(info.get("partition_key"), partitions):
                result.append(file_id)
        return result

    def _consumer_key(
        self, lake_table: str, consumer_id: str, view_id: str | None = None
    ) -> tuple[str, str, str]:
        return (lake_table, consumer_id, view_id or "")

    def consumer_exists(self, lake_table: str | None, consumer_id: str) -> bool:
        with self._lock:
            if lake_table:
                return self._consumer_key(lake_table, consumer_id) in self._consumers
            return any(key[1] == consumer_id for key in self._consumers)

    def register_consumer(
        self,
        consumer_id: str,
        lake_table: str,
        partitions: Any = None,
        view_id: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """登记或更新订阅方；确认位点严格按 表 + 消费方 + 视图 隔离。"""
        if partitions is None:
            partitions = (
                extra.pop("partition_keys", None)
                or extra.pop("partition_key", None)
                or extra.pop("partition_filters", None)
                or extra.pop("partition", None)
                or extra.pop("subscribed_partitions", None)
            )
        if not consumer_id or not lake_table:
            return {"ok": False, "success": False, "error": "consumer_id and lake_table are required"}
        parts = self.normalize_partitions(partitions)
        key = self._consumer_key(lake_table, str(consumer_id), view_id)
        with self._lock:
            consumer = self._consumers.get(key)
            if consumer is None:
                consumer = {
                    "consumer_id": str(consumer_id),
                    "lake_table": lake_table,
                    "view_id": view_id or "",
                    "partitions": parts,
                    "checkpoint_position": 0,
                    "checkpoint_seq": 0,
                    "checkpoint_snapshot": None,
                    "checkpoint_delivery_id": None,
                    "checkpoint_file_id": None,
                    "acknowledged_positions": [],
                    "acknowledged_file_ids": [],
                    "current_delivery_id": None,
                    "handoff_snapshot_id": None,
                    "delivery_ids": [],
                    "ledger": [],
                    "created_at": self._now(),
                    "updated_at": self._now(),
                }
                self._consumers[key] = consumer
            if partitions is not None:
                consumer["partitions"] = parts
            consumer["updated_at"] = self._now()
            for name, value in extra.items():
                if name not in {"consumer", "subscriber"}:
                    consumer[name] = value
            return self._consumer_public(consumer)

    def _get_consumer(
        self, consumer_id: str, lake_table: str | None = None, view_id: str | None = None
    ) -> dict[str, Any] | None:
        if lake_table is not None:
            return self._consumers.get(self._consumer_key(lake_table, consumer_id, view_id))
        matches = [c for key, c in self._consumers.items() if key[1] == consumer_id]
        if view_id is not None:
            matches = [c for c in matches if c.get("view_id") == view_id]
        return matches[-1] if matches else None

    def _consumer_public(self, consumer: dict[str, Any]) -> dict[str, Any]:
        return {
            "consumer_id": consumer["consumer_id"],
            "lake_table": consumer["lake_table"],
            "view_id": consumer.get("view_id") or None,
            "partitions": list(consumer.get("partitions") or []),
            "subscribed_to_all_partitions": not consumer.get("partitions"),
            "checkpoint_position": consumer["checkpoint_position"],
            "checkpoint_seq": consumer.get("checkpoint_seq", 0),
            "checkpoint_snapshot": consumer.get("checkpoint_snapshot"),
            "checkpoint_delivery_id": consumer.get("checkpoint_delivery_id"),
            "checkpoint_file_id": consumer.get("checkpoint_file_id"),
            "acknowledged_positions": list(consumer.get("acknowledged_positions") or []),
            "acknowledged_file_ids": list(consumer.get("acknowledged_file_ids") or []),
            "current_delivery_id": consumer.get("current_delivery_id"),
            "handoff_snapshot_id": consumer.get("handoff_snapshot_id"),
            "delivery_ids": list(consumer.get("delivery_ids") or []),
            "updated_at": consumer.get("updated_at"),
        }

    # ------------------------------------------------------------------
    # 快照绑定与交付生成
    # ------------------------------------------------------------------
    def bind_snapshot(
        self, lake_table: str, snapshot_id: str | None = None, consumer_id: str | None = None
    ) -> dict[str, Any]:
        """绑定指定可用快照；不传编号时绑定最新可用快照。"""
        if snapshot_id:
            snap = lake_store.get_snapshot(lake_table, snapshot_id)
            if snap is None:
                return {"ok": False, "success": False, "error": "snapshot not found",
                        "status_code": 404}
            if snap.status != "available":
                return {"ok": False, "success": False, "error": "snapshot is not available",
                        "status": snap.status, "status_code": 409}
        else:
            available = [s for s in lake_store.list_snapshots(lake_table) if s.status == "available"]
            if not available:
                return {"ok": False, "success": False, "error": "no available snapshot",
                        "status_code": 409}
            snap = available[-1]
        if consumer_id:
            consumer = self._get_consumer(consumer_id, lake_table)
            if consumer is not None:
                consumer["handoff_snapshot_id"] = snap.snapshot_id
                consumer["updated_at"] = self._now()
        return {
            "ok": True,
            "success": True,
            "lake_table": lake_table,
            "bound_snapshot": snap.snapshot_id,
            "snapshot_id": snap.snapshot_id,
            "status": snap.status,
            "file_ids": list(snap.file_ids),
            "bound_at": self._now(),
        }

    def _find_prior_addition(
        self, consumer: dict[str, Any], file_id: str
    ) -> tuple[Delivery | None, Change | None]:
        for delivery_id in reversed(consumer.get("delivery_ids") or []):
            old = self._deliveries.get(delivery_id)
            if not old:
                continue
            for change in reversed(old.get("changes") or []):
                if change.get("file_id") == file_id and change.get("change_type") == "added":
                    return old, change
        return None, None

    def create_delivery(
        self,
        lake_table: str,
        consumer_id: str,
        target_snapshot: Any = None,
        base_snapshot: Any = None,
        *,
        view_id: str | None = None,
        partitions: Any = None,
        partition_keys: Any = None,
        file_partitions: dict[str, str] | None = None,
        delivery_id: str | None = None,
        page_size: int = 1,
        **metadata: Any,
    ) -> dict[str, Any]:
        """生成一次按消费方隔离的快照交接明细。"""
        target_id = self.snapshot_id_of(target_snapshot)
        requested_base = self.snapshot_id_of(base_snapshot)
        with self._lock:
            consumer = self._get_consumer(consumer_id, lake_table, view_id)
            if consumer is None:
                consumer = self.register_consumer(
                    consumer_id, lake_table,
                    partitions=partitions if partitions is not None else partition_keys,
                    view_id=view_id,
                )
                # register_consumer 返回公开视图；重新取内部记录。
                consumer = self._get_consumer(consumer_id, lake_table, view_id)
            elif partitions is not None or partition_keys is not None:
                consumer["partitions"] = self.normalize_partitions(
                    partitions if partitions is not None else partition_keys
                )

            bind = self.bind_snapshot(lake_table, target_id)
            if not bind.get("success"):
                return bind
            target_id = bind["snapshot_id"]
            target = lake_store.get_snapshot(lake_table, target_id)
            assert target is not None

            base_id = requested_base or consumer.get("handoff_snapshot_id") or target.parent_snapshot
            overrides = dict(file_partitions or {})
            part_filters = consumer.get("partitions")

            base_visible = self._visible_file_ids(lake_table, base_id, part_filters, overrides)
            target_visible = self._visible_file_ids(lake_table, target_id, part_filters, overrides)
            base_set = set(base_visible)
            target_set = set(target_visible)
            added_ids = [fid for fid in target_visible if fid not in base_set]
            removed_ids = [fid for fid in base_visible if fid not in target_set]

            delivery_no = int(consumer.get("delivery_seq") or 0) + 1
            consumer["delivery_seq"] = delivery_no
            did = delivery_id or f"DLV-{delivery_no:04d}-{str(consumer_id)[:24]}"
            # 全局递增序号仅用于内部排序/排障；交付编号中的序号按消费方独立递增。
            global_no = self._next_no()
            # 允许调用方指定编号；若重复则返回既有交付，保证重试幂等。
            if did in self._deliveries:
                existing = self._deliveries[did]
                if existing.get("consumer_id") != consumer_id:
                    return {"ok": False, "success": False,
                            "error": "delivery_id already belongs to another consumer",
                            "status_code": 409}
                return self.delivery_detail(did, consumer_id)

            changes: list[Change] = []
            conflicts: list[dict[str, Any]] = []
            for file_id in removed_ids:
                old_delivery, prior = self._find_prior_addition(consumer, file_id)
                position = consumer.get("next_position", 0) + 1
                consumer["next_position"] = position
                info = self._file_info(lake_table, file_id, overrides)
                ever_delivered = prior is not None and bool(
                    prior.get("delivered", True) or prior.get("fetched")
                )
                change: Change = {
                    "change_id": f"{did}-CHG-{position}",
                    "delivery_id": did,
                    "position": position,
                    "seq": position,
                    "file_id": file_id,
                    "file": file_id,
                    "change_type": "withdrawn",
                    "type": "removed",
                    "operation": "撤回",
                    "action": "withdraw",
                    "partition_key": info.get("partition_key"),
                    "file_info": info,
                    "status": "pending",
                    "acknowledged": False,
                    "acked": False,
                    "delivered": True,
                    "ever_delivered": ever_delivered,
                    "recycled": info.get("recycled", False),
                    "prior_delivery_id": old_delivery.get("delivery_id") if old_delivery else None,
                    "prior_change_id": prior.get("change_id") if prior else None,
                    "prior_page_id": prior.get("page_id") if prior else None,
                    "prior_page_token": prior.get("page_token") if prior else None,
                    "ack_position": None,
                    "acknowledged_at": None,
                    "created_at": self._now(),
                }
                changes.append(change)
                if prior is not None and not prior.get("acknowledged"):
                    conflict = {
                        "file_id": file_id,
                        "type": "stale_in_flight_page",
                        "delivery_id": old_delivery.get("delivery_id"),
                        "page_id": prior.get("page_id"),
                        "page_token": prior.get("page_token"),
                        "replacement_change_id": change["change_id"],
                        "message": f"文件 {file_id} 的旧页尚未确认，但新快照已撤回该文件",
                    }
                    conflicts.append(conflict)
                    prior["status"] = "superseded"
                    prior["conflict"] = True
                    prior["superseded_by_delivery_id"] = did
                    prior["superseded_by_change_id"] = change["change_id"]
                    if old_delivery is not None:
                        old_delivery["status"] = "superseded"
                        old_delivery["conflict"] = True
                        old_delivery["superseded_by_delivery_id"] = did
                        old_delivery["superseded_at"] = self._now()
                consumer.setdefault("ledger", []).append({
                    "event_type": "withdrawal_offered",
                    "file_id": file_id,
                    "delivery_id": did,
                    "position": position,
                    "references_prior_delivery": change["prior_delivery_id"],
                    "ever_delivered": ever_delivered,
                    "at": self._now(),
                })

            for file_id in added_ids:
                # 已确认文件即使因基准缺失而出现在差异里，也绝不作为新文件再交付。
                if file_id in set(consumer.get("acknowledged_file_ids") or []):
                    continue
                position = consumer.get("next_position", 0) + 1
                consumer["next_position"] = position
                info = self._file_info(lake_table, file_id, overrides)
                change = {
                    "change_id": f"{did}-CHG-{position}",
                    "delivery_id": did,
                    "position": position,
                    "seq": position,
                    "file_id": file_id,
                    "file": file_id,
                    "change_type": "added",
                    "type": "added",
                    "operation": "新增",
                    "action": "add",
                    "partition_key": info.get("partition_key"),
                    "file_info": info,
                    "status": "delivered",
                    "acknowledged": False,
                    "acked": False,
                    "delivered_at": self._now(),
                    "fetched": False,
                    "fetch_count": 0,
                    "ack_position": None,
                    "acknowledged_at": None,
                    "created_at": self._now(),
                }
                changes.append(change)
                consumer.setdefault("ledger", []).append({
                    "event_type": "added_offered",
                    "file_id": file_id,
                    "delivery_id": did,
                    "position": position,
                    "at": self._now(),
                })

            changes.sort(key=lambda item: int(item["position"]))

            # 默认每个文件一页，便于表达“A 已确认、B 页未确认”。
            pages: list[dict[str, Any]] = []
            size = max(1, int(page_size or 1))
            for index in range(0, len(changes), size):
                chunk = changes[index:index + size]
                page_no = index // size + 1
                page_id = f"{did}-PAGE-{page_no}"
                token = secrets.token_urlsafe(18)
                page = {
                    "page_id": page_id,
                    "page_no": page_no,
                    "delivery_id": did,
                    "consumer_id": consumer_id,
                    "change_ids": [item["change_id"] for item in chunk],
                    "file_ids": [item["file_id"] for item in chunk],
                    "page_token": token,
                    "page_credential": token,
                    "credential": token,
                    "status": "delivered",
                    "acknowledged": False,
                    "ack_position": None,
                    "created_at": self._now(),
                }
                pages.append(page)
                token_record = {
                    "page_token": token,
                    "page_credential": token,
                    "delivery_id": did,
                    "page_id": page_id,
                    "consumer_id": consumer_id,
                    "consumer_key": (lake_table, consumer_id, view_id or ""),
                    "lake_table": lake_table,
                    "view_id": view_id or "",
                    "change_ids": page["change_ids"],
                    "file_ids": page["file_ids"],
                    "created_at": page["created_at"],
                    "used": False,
                }
                self._tokens[token] = token_record
                for change in chunk:
                    change["page_id"] = page_id
                    change["page_token"] = token
                    change["page_credential"] = token
                    change["credential"] = token

            added_out = [c["file_id"] for c in changes if c["change_type"] == "added"]
            withdrawn_out = [c["file_id"] for c in changes if c["change_type"] == "withdrawn"]
            delivery: Delivery = {
                "ok": True,
                "success": True,
                "delivery_id": did,
                "delivery_no": delivery_no,
                "global_delivery_no": global_no,
                "kind": "snapshot_handoff",
                "consumer_id": consumer_id,
                "lake_table": lake_table,
                "view_id": view_id,
                "partition_filters": list(part_filters or []),
                "subscribed_to_all_partitions": not part_filters,
                "base_snapshot": base_id,
                "previous_snapshot": base_id,
                "target_snapshot": target_id,
                "snapshot_id": target_id,
                "bound_snapshot": target_id,
                "base_file_ids": base_visible,
                "target_file_ids": target_visible,
                "changes": changes,
                "pages": pages,
                "added": added_out,
                "removed": withdrawn_out,
                "withdrawn": withdrawn_out,
                "new_files": added_out,
                "new_file_ids": added_out,
                "revoked_files": withdrawn_out,
                "deleted_files": withdrawn_out,
                "added_file_ids": added_out,
                "removed_file_ids": withdrawn_out,
                "withdrawn_file_ids": withdrawn_out,
                "added_files": added_out,
                "removed_files": withdrawn_out,
                "withdrawn_files": withdrawn_out,
                "additions": added_out,
                "deletions": withdrawn_out,
                "withdrawal_file_ids": withdrawn_out,
                "added_changes": [c for c in changes if c["change_type"] == "added"],
                "withdrawn_changes": [c for c in changes if c["change_type"] == "withdrawn"],
                "withdrawal_records": [c for c in changes if c["change_type"] == "withdrawn"],
                "change_records": changes,
                "conflicts": conflicts,
                "conflict": bool(conflicts),
                "has_conflict": bool(conflicts),
                "status": "active",
                "page_size": size,
                "metadata": metadata,
                "created_at": self._now(),
            }
            self._deliveries[did] = delivery
            self._delivery_order.append(did)
            consumer["current_delivery_id"] = did
            consumer["handoff_snapshot_id"] = target_id
            consumer.setdefault("delivery_ids", []).append(did)
            consumer["updated_at"] = self._now()
            self._recompute_checkpoint(consumer)
            return self._jsonable(delivery)

    def refresh_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _jsonable(self, value: Any) -> Any:
        # 当前结构全部由 list/dict/标量组成；单独提供出口便于以后扩展 dataclass。
        return value

    def list_deliveries(
        self,
        consumer_id: str | None = None,
        lake_table: str | None = None,
        view_id: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = []
            for did in self._delivery_order:
                item = self._deliveries[did]
                if consumer_id and item.get("consumer_id") != consumer_id:
                    continue
                if lake_table and item.get("lake_table") != lake_table:
                    continue
                if view_id is not None and item.get("view_id") != view_id:
                    continue
                rows.append(self.delivery_detail(did))
            return rows

    def delivery_detail(
        self, delivery_id: str, consumer_id: str | None = None
    ) -> dict[str, Any]:
        with self._lock:
            delivery = self._deliveries.get(str(delivery_id))
            if delivery is None and str(delivery_id).isdigit():
                for candidate in self._delivery_order:
                    item = self._deliveries[candidate]
                    number_matches = (
                        int(item.get("delivery_no", -1)) == int(delivery_id)
                        or int(item.get("global_delivery_no", -1)) == int(delivery_id)
                    )
                    if not number_matches:
                        continue
                    if consumer_id is not None and item.get("consumer_id") != consumer_id:
                        continue
                    delivery = item
                    break
            if delivery is None:
                return {"ok": False, "success": False, "error": "delivery not found",
                        "delivery_id": delivery_id, "status_code": 404}
            if consumer_id is not None and delivery.get("consumer_id") != consumer_id:
                return {"ok": False, "success": False, "error": "delivery belongs to another consumer",
                        "delivery_id": delivery_id, "status_code": 403}
            result = dict(delivery)
            result["checkpoint"] = self.checkpoint(
                delivery["lake_table"], delivery["consumer_id"], delivery.get("view_id")
            )
            return self._jsonable(result)

    def get_delivery(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return self.delivery_detail(delivery_id, consumer_id)

    def checkpoint(
        self, lake_table: str, consumer_id: str, view_id: str | None = None
    ) -> dict[str, Any]:
        with self._lock:
            consumer = self._get_consumer(consumer_id, lake_table, view_id)
            if consumer is None:
                return {
                    "ok": True,
                    "success": True,
                    "consumer_id": consumer_id,
                    "lake_table": lake_table,
                    "position": 0,
                    "checkpoint_position": 0,
                    "snapshot_id": None,
                    "acknowledged_file_ids": [],
                }
            return {
                "ok": True,
                "success": True,
                **self._consumer_public(consumer),
                "position": consumer["checkpoint_position"],
                "snapshot_id": consumer.get("checkpoint_snapshot"),
            }

    def verify_credential(
        self,
        page_token: str,
        consumer_id: str | None = None,
        delivery_id: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            record = self._tokens.get(page_token)
            if record is None:
                return {"ok": False, "success": False, "valid": False,
                        "error": "page credential not found", "status_code": 400}
            if consumer_id is not None and record["consumer_id"] != consumer_id:
                return {"ok": False, "success": False, "valid": False,
                        "error": "page credential does not belong to this consumer",
                        "reason": "consumer_mismatch", "status_code": 403}
            if delivery_id is not None and record["delivery_id"] != delivery_id:
                return {"ok": False, "success": False, "valid": False,
                        "error": "page credential does not belong to this delivery",
                        "reason": "delivery_mismatch", "status_code": 409}
            return {"ok": True, "success": True, "valid": True, **record}

    def _find_page(
        self, delivery: Delivery, page_id: str | None = None, page_token: str | None = None
    ) -> dict[str, Any] | None:
        for page in delivery.get("pages") or []:
            if page_id is not None and page.get("page_id") == page_id:
                return page
            if page_token is not None and page.get("page_token") == page_token:
                return page
        return None

    def fetch_page(
        self,
        *,
        consumer_id: str | None = None,
        lake_table: str | None = None,
        delivery_id: str | None = None,
        page_id: str | None = None,
        page_token: str | None = None,
        allow_stale: bool = False,
        **_: Any,
    ) -> dict[str, Any]:
        with self._lock:
            token_record = None
            token = page_token
            if token:
                token_record = self._tokens.get(token)
                if token_record is None:
                    return {"ok": False, "success": False, "valid": False,
                            "error": "invalid page credential", "status_code": 400}
                if consumer_id and token_record["consumer_id"] != consumer_id:
                    return {"ok": False, "success": False, "valid": False,
                            "error": "page credential belongs to another consumer",
                            "status_code": 403}
                consumer_id = token_record["consumer_id"]
                delivery_id = delivery_id or token_record["delivery_id"]
                page_id = page_id or token_record["page_id"]
                lake_table = lake_table or token_record["lake_table"]
            if not delivery_id:
                return {"ok": False, "success": False, "error": "delivery_id is required",
                        "status_code": 400}
            delivery = self._deliveries.get(delivery_id)
            if delivery is None:
                return {"ok": False, "success": False, "error": "delivery not found",
                        "status_code": 404}
            if consumer_id and delivery["consumer_id"] != consumer_id:
                return {"ok": False, "success": False, "error": "delivery belongs to another consumer",
                        "status_code": 403}
            page = self._find_page(delivery, page_id, token)
            if page is None:
                return {"ok": False, "success": False, "error": "page not found",
                        "status_code": 404}

            stale = delivery.get("status") == "superseded"
            if stale and not allow_stale and not page.get("acknowledged"):
                current = None
                consumer = self._get_consumer(delivery["consumer_id"], delivery["lake_table"])
                if consumer and consumer.get("current_delivery_id"):
                    current = self.delivery_detail(consumer["current_delivery_id"])
                return {
                    "ok": False,
                    "success": False,
                    "conflict": True,
                    "status_code": 409,
                    "error": "delivery_page_superseded",
                    "message": "旧页所属交付已被新快照交接取代，请按新交付中的撤回/新增明细续取",
                    "delivery_id": delivery_id,
                    "page": page,
                    "current_delivery": current,
                }
            page["fetched"] = True
            first_fetch = not page.get("first_fetched_at")
            page["fetch_count"] = int(page.get("fetch_count", 0)) + 1
            page["last_fetched_at"] = self._now()
            if first_fetch:
                page["first_fetched_at"] = page["last_fetched_at"]
            for change in delivery["changes"]:
                if change["change_id"] in page["change_ids"]:
                    change["fetched"] = True
                    change["fetch_count"] = int(change.get("fetch_count", 0)) + 1
            consumer = self._get_consumer(delivery["consumer_id"], delivery["lake_table"])
            if consumer is not None and first_fetch:
                consumer.setdefault("ledger", []).append({
                    "event_type": "page_delivered",
                    "delivery_id": delivery_id,
                    "page_id": page_id,
                    "page_token": page.get("page_token"),
                    "file_ids": list(page.get("file_ids") or []),
                    "file_id": page["file_ids"][0] if len(page.get("file_ids") or []) == 1 else None,
                    "at": page["last_fetched_at"],
                })
            return {"ok": True, "success": True, "resumed": True, "delivery_id": delivery_id,
                    "page": page, "page_token": page.get("page_token"),
                    "checkpoint": self.checkpoint(delivery["lake_table"], delivery["consumer_id"])}

    # ------------------------------------------------------------------
    # 确认位点
    # ------------------------------------------------------------------
    def _recompute_checkpoint(self, consumer: dict[str, Any]) -> None:
        changes: list[Change] = []
        for did in consumer.get("delivery_ids") or []:
            delivery = self._deliveries.get(did)
            if delivery:
                changes.extend(delivery.get("changes") or [])
        by_position = {int(c["position"]): c for c in changes if "position" in c}
        checkpoint = 0
        acknowledged_files: list[str] = []
        for position in sorted(by_position):
            if position != checkpoint + 1:
                break
            change = by_position[position]
            resolved = bool(change.get("acknowledged"))
            if not resolved and change.get("change_type") == "added" and change.get("status") == "superseded":
                replacement_id = change.get("superseded_by_change_id")
                replacement = next(
                    (c for c in changes if c.get("change_id") == replacement_id), None
                )
                resolved = bool(replacement and replacement.get("acknowledged"))
                if resolved:
                    change["canceled_by_ack_position"] = replacement.get("ack_position")
            if not resolved:
                break
            checkpoint = position
            if change.get("change_type") == "added" and change.get("acknowledged"):
                fid = change["file_id"]
                if fid not in acknowledged_files:
                    acknowledged_files.append(fid)
        consumer["checkpoint_position"] = checkpoint
        consumer["checkpoint_seq"] = checkpoint
        acked_positions = sorted({
            int(c["position"]) for c in changes if c.get("acknowledged") and "position" in c
        })
        consumer["acknowledged_positions"] = acked_positions
        consumer["acknowledged_file_ids"] = acknowledged_files
        current = by_position.get(checkpoint)
        if current:
            current_did = current.get("delivery_id")
            current_delivery = self._deliveries.get(current_did or "")
            consumer["checkpoint_snapshot"] = (
                current_delivery.get("target_snapshot") if current_delivery else None
            )
            consumer["checkpoint_delivery_id"] = current_did
            consumer["checkpoint_file_id"] = current.get("file_id")
        else:
            consumer["checkpoint_snapshot"] = None
            consumer["checkpoint_delivery_id"] = None
            consumer["checkpoint_file_id"] = None

    def acknowledge(self, **params: Any) -> dict[str, Any]:
        with self._lock:
            consumer_id = params.get("consumer_id")
            lake_table = params.get("lake_table")
            delivery_id = params.get("delivery_id")
            page_id = params.get("page_id")
            change_id = params.get("change_id")
            file_id = params.get("file_id")
            token = params.get("page_token") or params.get("page_credential") or params.get("credential") or params.get("token")
            ack_position = params.get("ack_position") or params.get("position") or params.get("checkpoint_position")
            if ack_position is not None and not isinstance(ack_position, (int, float)):
                # 允许 checkpoint 传成数字字符串。
                try:
                    ack_position = int(ack_position)
                except (TypeError, ValueError):
                    ack_position = None
            allow_stale = bool(params.get("allow_stale") or params.get("force") or False)

            # 允许用消费方内 delivery_no 确认；先解析成全局唯一 delivery_id。
            if delivery_id is not None and not self._deliveries.get(str(delivery_id)) \
                    and str(delivery_id).isdigit():
                numeric_detail = self.delivery_detail(str(delivery_id), consumer_id)
                if numeric_detail.get("success"):
                    delivery_id = numeric_detail["delivery_id"]

            token_record = self._tokens.get(token) if token else None
            if token and token_record is None:
                return {"ok": False, "success": False, "valid": False,
                        "error": "page credential not found", "status_code": 400}
            if token_record:
                if consumer_id and token_record["consumer_id"] != consumer_id:
                    return {"ok": False, "success": False, "valid": False,
                            "error": "page credential does not belong to this consumer",
                            "reason": "consumer_mismatch", "status_code": 403}
                consumer_id = token_record["consumer_id"]
                if delivery_id and token_record["delivery_id"] != delivery_id:
                    return {"ok": False, "success": False, "valid": False,
                            "error": "page credential does not belong to this delivery",
                            "reason": "delivery_mismatch", "status_code": 409}
                delivery_id = token_record["delivery_id"]
                page_id = page_id or token_record["page_id"]
                lake_table = lake_table or token_record["lake_table"]

            if not consumer_id:
                return {"ok": False, "success": False, "error": "consumer_id is required",
                        "status_code": 400}
            if not delivery_id:
                return {"ok": False, "success": False, "error": "delivery_id or page_token is required",
                        "status_code": 400}
            delivery = self._deliveries.get(str(delivery_id))
            if delivery is None and str(delivery_id).isdigit():
                found = self.delivery_detail(str(delivery_id), consumer_id)
                if found.get("success"):
                    delivery = self._deliveries[found["delivery_id"]]
                    delivery_id = delivery["delivery_id"]
            if delivery is None:
                return {"ok": False, "success": False, "error": "delivery not found",
                        "status_code": 404}
            if consumer_id is None:
                # 交付编号本身已界定消费方；调用方未重复传消费方时按交付归属处理。
                consumer_id = delivery["consumer_id"]
            elif delivery["consumer_id"] != consumer_id:
                return {"ok": False, "success": False, "error": "delivery belongs to another consumer",
                        "reason": "consumer_mismatch", "status_code": 403}
            if lake_table and delivery["lake_table"] != lake_table:
                return {"ok": False, "success": False, "error": "lake_table mismatch",
                        "status_code": 400}
            lake_table = delivery["lake_table"]
            consumer = self._get_consumer(consumer_id, lake_table, delivery.get("view_id"))
            if consumer is None:
                return {"ok": False, "success": False, "error": "consumer checkpoint not found",
                        "status_code": 404}

            page = self._find_page(delivery, page_id, token) if page_id or token else None
            if token_record and page is None:
                return {"ok": False, "success": False, "error": "credential page not found",
                        "status_code": 404}

            target_changes: list[Change]
            if page is not None:
                target_ids = set(page["change_ids"])
                target_changes = [c for c in delivery["changes"] if c["change_id"] in target_ids]
            elif change_id:
                target_changes = [c for c in delivery["changes"] if c["change_id"] == change_id]
            elif file_id:
                target_changes = [c for c in delivery["changes"] if c["file_id"] == file_id]
            elif ack_position is not None:
                target_changes = [c for c in delivery["changes"] if int(c["position"]) <= int(ack_position)]
            else:
                return {"ok": False, "success": False,
                        "error": "page_id, page_token, change_id, file_id or ack_position is required",
                        "status_code": 400}
            if not target_changes:
                return {"ok": False, "success": False, "error": "no matching changes to acknowledge",
                        "status_code": 404}

            unacked = [c for c in target_changes if not c.get("acknowledged")]
            stale = delivery.get("status") == "superseded"
            if stale and unacked and not allow_stale:
                current = None
                if consumer.get("current_delivery_id"):
                    current = self.delivery_detail(consumer["current_delivery_id"], consumer_id)
                return {
                    "ok": False,
                    "success": False,
                    "conflict": True,
                    "status_code": 409,
                    "valid_credential": token_record is not None,
                    "error": "delivery_page_superseded",
                    "message": "不能把旧交付页作为当前确认位置；请改确认新交付中的撤回/新增明细",
                    "delivery_id": delivery_id,
                    "target_changes": target_changes,
                    "current_delivery": current,
                }

            now = self._now()
            for change in target_changes:
                if change.get("acknowledged"):
                    continue
                change["acknowledged"] = True
                change["acked"] = True
                change["status"] = "acknowledged"
                change["acknowledged_at"] = now
                change["ack_position"] = int(change["position"])
                change["client_checkpoint"] = params.get("checkpoint")
                if allow_stale and stale:
                    change["stale_acknowledged"] = True
                consumer.setdefault("ledger", []).append({
                    "event_type": f"{change['change_type']}_acknowledged",
                    "file_id": change["file_id"],
                    "delivery_id": delivery_id,
                    "position": change["position"],
                    "ack_position": change["position"],
                    "page_id": change.get("page_id"),
                    "page_token": change.get("page_token"),
                    "at": now,
                })

            if page is not None:
                page_targets = [c for c in delivery["changes"] if c["change_id"] in page["change_ids"]]
                if all(c.get("acknowledged") for c in page_targets):
                    page["acknowledged"] = True
                    page["status"] = "acknowledged"
                    page["acknowledged_at"] = now
                    page["ack_position"] = max(int(c.get("ack_position") or 0) for c in page_targets)
                if token_record:
                    token_record["used"] = True
                    token_record["acknowledged_at"] = now

            if all(c.get("acknowledged") for c in delivery["changes"]):
                delivery["status"] = "acknowledged"
                delivery["acknowledged_at"] = now
            elif delivery.get("status") == "active":
                delivery["status"] = "partially_acknowledged"

            self._recompute_checkpoint(consumer)
            checkpoint = self.checkpoint(lake_table, consumer_id, delivery.get("view_id"))
            return {
                "ok": True,
                "success": True,
                "acknowledged": True,
                "acked": True,
                "idempotent": len(unacked) == 0,
                "delivery_id": delivery_id,
                "page_id": page.get("page_id") if page else None,
                "acknowledged_change_ids": [c["change_id"] for c in target_changes],
                "acknowledged_file_ids": [c["file_id"] for c in target_changes],
                "ack_position": target_changes[-1].get("ack_position"),
                "position": target_changes[-1].get("ack_position"),
                "checkpoint": checkpoint,
                "checkpoint_position": checkpoint["checkpoint_position"],
                "confirmed_position": checkpoint["checkpoint_position"],
                "committed_position": checkpoint["checkpoint_position"],
                "ack_cursor": checkpoint,
                "stale_delivery": stale,
            }

    def reconnect(self, **params: Any) -> dict[str, Any]:
        consumer_id = params.get("consumer_id")
        lake_table = params.get("lake_table")
        token = params.get("page_token") or params.get("page_credential") or params.get("credential")
        delivery_id = params.get("delivery_id")
        with self._lock:
            token_record = self._tokens.get(token) if token else None
            if token and token_record is None:
                return {"ok": False, "success": False, "valid": False,
                        "error": "invalid page credential", "status_code": 400}
            if token_record:
                if consumer_id and token_record["consumer_id"] != consumer_id:
                    return {"ok": False, "success": False, "valid": False,
                            "error": "page credential belongs to another consumer",
                            "status_code": 403}
                consumer_id = token_record["consumer_id"]
                lake_table = lake_table or token_record["lake_table"]
                delivery_id = delivery_id or token_record["delivery_id"]
            if not consumer_id:
                return {"ok": False, "success": False, "error": "consumer_id is required",
                        "status_code": 400}
            if not lake_table and delivery_id:
                delivery = self._deliveries.get(delivery_id)
                if delivery:
                    lake_table = delivery["lake_table"]
            if not lake_table:
                return {"ok": False, "success": False, "error": "lake_table is required",
                        "status_code": 400}

            consumer = self._get_consumer(consumer_id, lake_table)
            if consumer is None:
                self.register_consumer(consumer_id, lake_table, params.get("partitions"))
                consumer = self._get_consumer(consumer_id, lake_table)

            # 若湖表已有更新的可用快照，重连时自动形成新交付并处理旧页冲突。
            if params.get("auto_refresh", True):
                available = [s for s in lake_store.list_snapshots(lake_table) if s.status == "available"]
                latest = available[-1].snapshot_id if available else None
                if latest and consumer.get("handoff_snapshot_id") != latest:
                    created = self.create_delivery(
                        lake_table, consumer_id, latest,
                        base_snapshot=consumer.get("handoff_snapshot_id"),
                        view_id=consumer.get("view_id"),
                    )
                    if not created.get("success"):
                        return created

            checkpoint = self.checkpoint(lake_table, consumer_id)
            current_id = consumer.get("current_delivery_id")
            current = self.delivery_detail(current_id, consumer_id) if current_id else None
            resumed_page = None
            conflict = False
            stale_delivery = None
            if token_record and delivery_id:
                fetched = self.fetch_page(
                    consumer_id=consumer_id, lake_table=lake_table,
                    delivery_id=delivery_id, page_token=token, allow_stale=True,
                )
                resumed_page = fetched.get("page")
                old_delivery = self._deliveries.get(delivery_id)
                if old_delivery and old_delivery.get("status") == "superseded":
                    conflict = True
                    stale_delivery = self.delivery_detail(delivery_id, consumer_id)

            pending_changes: list[dict[str, Any]] = []
            if current and current.get("success"):
                for change in current.get("changes", []):
                    if not change.get("acknowledged"):
                        pending_changes.append(change)
            return {
                "ok": True,
                "success": True,
                "connected": True,
                "resumed": resumed_page is not None,
                "conflict": conflict,
                "consumer": self._consumer_public(consumer),
                "checkpoint": checkpoint,
                "resumed_page": resumed_page,
                "stale_delivery": stale_delivery,
                "current_delivery": current,
                "delivery": current,
                "pending_changes": pending_changes,
                "pending_file_ids": [c["file_id"] for c in pending_changes],
                "pending_added_file_ids": [
                    c["file_id"] for c in pending_changes if c.get("change_type") == "added"
                ],
                "pending_withdrawn_file_ids": [
                    c["file_id"] for c in pending_changes if c.get("change_type") == "withdrawn"
                ],
                "reconnect_at": self._now(),
            }

    # ------------------------------------------------------------------
    # 概览、履历与回放
    # ------------------------------------------------------------------
    def consumer_history(self, consumer_id: str, lake_table: str | None = None) -> dict[str, Any]:
        with self._lock:
            consumers = [self._get_consumer(consumer_id, lake_table)] if lake_table else [
                c for key, c in self._consumers.items() if key[1] == consumer_id
            ]
            consumers = [c for c in consumers if c]
            if not consumers:
                return {"ok": False, "success": False, "error": "consumer not found",
                        "status_code": 404}
            histories = []
            for consumer in consumers:
                histories.append({
                    "consumer_id": consumer["consumer_id"],
                    "lake_table": consumer["lake_table"],
                    "view_id": consumer.get("view_id"),
                    "checkpoint": self.checkpoint(
                        consumer["lake_table"], consumer["consumer_id"], consumer.get("view_id")
                    ),
                    "ledger": list(consumer.get("ledger") or []),
                    "deliveries": self.list_deliveries(
                        consumer["consumer_id"], consumer["lake_table"], consumer.get("view_id")
                    ),
                })
            return {"ok": True, "success": True, "consumer_id": consumer_id, "histories": histories,
                    "history": histories[0]["ledger"] if len(histories) == 1 else histories}

    def overview(
        self,
        lake_table: str | None = None,
        consumer_id: str | None = None,
        view_id: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            keys = list(self._consumers.keys())
            if lake_table:
                keys = [key for key in keys if key[0] == lake_table]
            if consumer_id:
                keys = [key for key in keys if key[1] == consumer_id]
            if view_id is not None:
                keys = [key for key in keys if key[2] == (view_id or "")]

            consumer_views = []
            all_retained: list[dict[str, Any]] = []
            all_withdrawn: list[dict[str, Any]] = []
            for key in keys:
                consumer = self._consumers[key]
                table = consumer["lake_table"]
                target_id = consumer.get("handoff_snapshot_id")
                if not target_id:
                    available = [s for s in lake_store.list_snapshots(table) if s.status == "available"]
                    target_id = available[-1].snapshot_id if available else None
                ids = self._visible_file_ids(table, target_id, consumer.get("partitions"))
                retained = []
                for fid in ids:
                    item = self._file_info(table, fid)
                    item.update({
                        "status": "retained",
                        "delivery_status": "当前留存",
                        "consumer_id": consumer["consumer_id"],
                    })
                    retained.append(item)

                withdrawn_map: dict[str, dict[str, Any]] = {}
                for did in consumer.get("delivery_ids") or []:
                    delivery = self._deliveries.get(did)
                    if not delivery:
                        continue
                    for change in delivery.get("changes") or []:
                        if change.get("change_type") != "withdrawn":
                            continue
                        fid = change["file_id"]
                        info = self._file_info(table, fid)
                        withdrawn_map[fid] = FileView({
                            "file_id": fid,
                            "file": fid,
                            "status": "withdrawn",
                            "delivery_status": "已经撤回",
                            "consumer_id": consumer["consumer_id"],
                            "lake_table": table,
                            "partition_key": change.get("partition_key") or info.get("partition_key"),
                            "withdrawn_in_delivery_id": did,
                            "withdrawal_change_id": change.get("change_id"),
                            "acknowledged": change.get("acknowledged", False),
                            "ack_position": change.get("ack_position"),
                            "recycled": info.get("recycled", change.get("recycled", False)),
                            "ever_delivered": change.get("ever_delivered", False),
                            "original_delivery_id": change.get("prior_delivery_id"),
                            "original_page_id": change.get("prior_page_id"),
                            "original_page_token": change.get("prior_page_token"),
                        })
                withdrawn = list(withdrawn_map.values())
                view = {
                    "consumer_id": consumer["consumer_id"],
                    "lake_table": table,
                    "view_id": consumer.get("view_id"),
                    "partitions": consumer.get("partitions") or [],
                    "checkpoint_position": consumer["checkpoint_position"],
                    "checkpoint_snapshot": consumer.get("checkpoint_snapshot"),
                    "current_delivery_id": consumer.get("current_delivery_id"),
                    "retained_files": retained,
                    "current_files": retained,
                    "retained_file_ids": [item["file_id"] for item in retained],
                    "current_file_ids": [item["file_id"] for item in retained],
                    "withdrawn_files": withdrawn,
                    "withdrawn_file_ids": [item["file_id"] for item in withdrawn],
                    "retained_count": len(retained),
                    "withdrawn_count": len(withdrawn),
                }
                consumer_views.append(view)
                all_retained.extend(retained)
                all_withdrawn.extend(withdrawn)

            # 未指定消费方但指定表时，也给出表级最新快照留存清单。
            table_retained: list[dict[str, Any]] = []
            if lake_table and not consumer_views:
                available = [s for s in lake_store.list_snapshots(lake_table) if s.status == "available"]
                if available:
                    for fid in available[-1].file_ids:
                        item = self._file_info(lake_table, fid)
                        item.update({"status": "retained", "delivery_status": "当前留存"})
                        table_retained.append(item)
                all_retained = table_retained

            return {
                "ok": True,
                "success": True,
                "lake_table": lake_table,
                "consumer_id": consumer_id,
                "consumers": consumer_views,
                "items": consumer_views,
                "retained_files": all_retained,
                "current_files": all_retained,
                "retained_file_ids": [item["file_id"] for item in all_retained],
                "current_file_ids": [item["file_id"] for item in all_retained],
                "withdrawn_files": all_withdrawn,
                "withdrawn_file_ids": [item["file_id"] for item in all_withdrawn],
                "retained_count": len(all_retained),
                "withdrawn_count": len(all_withdrawn),
                "total": len(consumer_views),
            }

    def replay_delivery(
        self, delivery_id: str, consumer_id: str | None = None
    ) -> dict[str, Any]:
        delivery = self._deliveries.get(str(delivery_id))
        if delivery is None and str(delivery_id).isdigit():
            detail = self.delivery_detail(str(delivery_id), consumer_id)
            if not detail.get("success"):
                return detail
            delivery = self._deliveries[detail["delivery_id"]]
        if delivery is None:
            return {"ok": False, "success": False, "error": "delivery not found", "status_code": 404}
        if consumer_id and delivery["consumer_id"] != consumer_id:
            return {"ok": False, "success": False, "error": "delivery belongs to another consumer",
                    "status_code": 403}

        table = delivery["lake_table"]
        base_ids = list(delivery.get("base_file_ids") or self._snapshot_file_ids(
            table, delivery.get("base_snapshot")
        ))
        confirmed = [c for c in delivery.get("changes", []) if c.get("acknowledged")]
        replayed = list(base_ids)
        acknowledged_added: list[str] = []
        withdrawals: list[dict[str, Any]] = []
        for change in sorted(confirmed, key=lambda c: int(c["position"])):
            fid = change["file_id"]
            if change["change_type"] == "added":
                if fid not in replayed:
                    replayed.append(fid)
                acknowledged_added.append(fid)
            elif change["change_type"] == "withdrawn":
                if fid in replayed:
                    replayed.remove(fid)
                withdrawals.append({
                    "file_id": fid,
                    "withdrawn": True,
                    "acknowledged": True,
                    "ack_position": change.get("ack_position"),
                    "position": change.get("position"),
                    "recycled": self._file_info(table, fid).get("recycled", False),
                    "prior_delivery_id": change.get("prior_delivery_id"),
                })

        target_id = delivery.get("target_snapshot")
        target_all = self._snapshot_file_ids(table, target_id)
        filters = delivery.get("partition_filters") or None
        target_scoped = self._visible_file_ids(table, target_id, filters)
        target_order = target_scoped or target_all
        ordered = [fid for fid in target_order if fid in replayed]
        ordered.extend(fid for fid in replayed if fid not in ordered)
        all_confirmed = all(c.get("acknowledged") for c in delivery.get("changes", []))
        match_scoped = ordered == target_scoped
        match_all = ordered == target_all
        return {
            "ok": True,
            "success": True,
            "delivery_id": delivery["delivery_id"],
            "delivery_no": delivery.get("delivery_no"),
            "consumer_id": delivery["consumer_id"],
            "lake_table": table,
            "base_snapshot": delivery.get("base_snapshot"),
            "snapshot_id": target_id,
            "target_snapshot": target_id,
            "confirmed_changes": confirmed,
            "confirmed_change_ids": [c["change_id"] for c in confirmed],
            "acknowledged_files": acknowledged_added,
            "confirmed_files": ordered,
            "replayed_file_ids": ordered,
            "file_ids": ordered,
            "files": [self._file_info(table, fid) for fid in ordered],
            "file_details": [self._file_info(table, fid) for fid in ordered],
            "withdrawals": withdrawals,
            "withdrawn_markers": withdrawals,
            "timeline": [
                {
                    "position": c.get("position"),
                    "file_id": c.get("file_id"),
                    "change_type": c.get("change_type"),
                    "acknowledged": c.get("acknowledged"),
                    "ack_position": c.get("ack_position"),
                    "status": c.get("status"),
                }
                for c in delivery.get("changes", [])
            ],
            "snapshot_file_ids": target_all,
            "partition_scoped_snapshot_file_ids": target_scoped,
            "fully_confirmed": all_confirmed,
            "match_snapshot": match_scoped if filters else match_all,
            "match_partition_scoped_snapshot": match_scoped,
            "match_full_snapshot": match_all,
            "replayed_at": self._now(),
        }


delivery_store = DeliveryStore()
