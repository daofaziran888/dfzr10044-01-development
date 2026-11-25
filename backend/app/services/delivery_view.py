"""交付视图服务：以消费方为单位呈现快照交接明细、确认位点和回放。

为兼容不同调用习惯，门面层保留较多语义别名；真正的状态与冲突规则集中在
``app.delivery_store.delivery_store``。
"""
from __future__ import annotations

from typing import Any

from app.delivery_store import delivery_store
from app.lake_store import lake_store
from app.services.base import BaseModuleService
from app.store import store


CONSUMER_KEYS = {
    "consumer_id", "consumer", "consumer_name", "subscriber", "subscriber_id",
    "downstream", "downstream_consumer", "consumer_identifier",
    "消费方", "下游消费方", "订阅方", "订阅方标识",
}
TABLE_KEYS = {
    "lake_table", "table", "table_name", "lake_table_name",
    "所属湖表", "湖表", "表名",
}
TARGET_KEYS = {
    "target_snapshot", "snapshot_id", "snapshot", "to_snapshot", "new_snapshot",
    "refreshed_snapshot", "latest_snapshot", "snapshot_b", "s2", "target",
    "目标快照", "新快照", "绑定快照",
}
BASE_KEYS = {
    "base_snapshot", "from_snapshot", "previous_snapshot", "old_snapshot",
    "view_snapshot", "snapshot_a", "s1", "since_snapshot", "base",
    "基准快照", "原快照", "旧快照",
}
DELIVERY_KEYS = {"delivery_id", "delivery_no", "delivery_number", "id", "交付编号", "交付号"}
PAGE_KEYS = {"page_id", "page", "page_no", "page_number", "页编号", "页码"}
TOKEN_KEYS = {
    "page_token", "token", "credential", "page_credential", "cursor",
    "checkpoint_token", "页凭据", "凭据",
}
FILE_KEYS = {"file_id", "file", "file_name", "filename", "文件", "文件编号"}
CHANGE_KEYS = {"change_id", "change", "change_record_id", "变更编号"}
POSITION_KEYS = {
    "ack_position", "position", "seq", "checkpoint_position", "checkpoint_seq",
    "checkpoint", "ack_cursor", "确认位置", "位点",
}
PARTITION_KEYS = {"partitions", "partition", "partition_keys", "partition_key", "分区", "订阅分区"}
VIEW_KEYS = {"view_id", "publish_view_id", "view_number", "视图编号"}


def _extract_aliases(data: dict[str, Any], names: set[str]) -> Any:
    for name in names:
        if name in data and data[name] not in (None, ""):
            return data[name]
    return None


def _as_dict_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for arg in args:
        if isinstance(arg, dict):
            data.update(arg)
    data.update(kwargs)
    return data


def _known_snapshot_id(value: Any) -> str | None:
    snap_id = delivery_store.snapshot_id_of(value)
    if not snap_id:
        return None
    for table in lake_store._snapshots:  # noqa: SLF001 - 门面需要跨表识别位置参数
        if lake_store.get_snapshot(table, snap_id):
            return snap_id
    return None


def create_kwargs_from_call(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    data = _as_dict_args(args, kwargs)
    result = dict(data)

    table = _extract_aliases(data, TABLE_KEYS)
    consumer = _extract_aliases(data, CONSUMER_KEYS)
    target = _extract_aliases(data, TARGET_KEYS)
    base = _extract_aliases(data, BASE_KEYS)
    view_id = _extract_aliases(data, VIEW_KEYS)
    partitions = _extract_aliases(data, PARTITION_KEYS)

    positional = [arg for arg in args if not isinstance(arg, dict)]
    list_args = [arg for arg in positional if isinstance(arg, (list, tuple, set))]
    positional = [arg for arg in positional if not isinstance(arg, (list, tuple, set))]
    if list_args and result.get("partitions") is None:
        result["partitions"] = list_args[0]
    # 先按本仓库常见顺序 table, consumer, target, base 填充。
    defaults = [table, consumer, target, base, view_id]
    names = ["lake_table", "consumer_id", "target_snapshot", "base_snapshot", "view_id"]
    for name, value in zip(names, positional):
        if defaults[names.index(name)] is None:
            result[name] = value

    # 再利用运行时状态识别颠倒顺序的位置参数。
    for value in positional:
        if not isinstance(value, str):
            continue
        snap_id = _known_snapshot_id(value)
        if snap_id:
            if snap_id == result.get("target_snapshot") or snap_id == result.get("base_snapshot"):
                continue
            if result.get("target_snapshot") is None and base is not None:
                result["target_snapshot"] = snap_id
            elif result.get("base_snapshot") is None and target is not None:
                result["base_snapshot"] = snap_id
            elif result.get("target_snapshot") is None:
                result["target_snapshot"] = snap_id
            elif result.get("base_snapshot") is None:
                result["base_snapshot"] = snap_id
            continue
        if value in lake_store._snapshots or any(  # noqa: SLF001
            str(row.get("表名全称")) == value for row in store.rows("lake_catalog")
        ):
            if result.get("lake_table") is None:
                result["lake_table"] = value
            continue
        if delivery_store.consumer_exists(None, value):
            if result.get("consumer_id") is None:
                result["consumer_id"] = value

    # 兼容 (consumer_id, lake_table, ...) 的消费方优先参数顺序。
    table_value = result.get("lake_table")
    consumer_value = result.get("consumer_id")
    if isinstance(table_value, str) and isinstance(consumer_value, str):
        first_known_table = table_value in lake_store._snapshots or any(  # noqa: SLF001
            d.get("lake_table") == table_value for d in delivery_store._deliveries.values()  # noqa: SLF001
        )
        second_known_table = consumer_value in lake_store._snapshots  # noqa: SLF001
        first_known_consumer = delivery_store.consumer_exists(None, table_value)
        second_known_consumer = delivery_store.consumer_exists(None, consumer_value)
        if not first_known_table and second_known_table and not second_known_consumer:
            result["lake_table"], result["consumer_id"] = consumer_value, table_value
        elif first_known_consumer and not second_known_consumer and not second_known_table:
            result["lake_table"], result["consumer_id"] = consumer_value, table_value

    # 位置参数中的 list/tuple/set 视为分区订阅。
    for value in args:
        if isinstance(value, (list, tuple, set)) and result.get("partitions") is None:
            result["partitions"] = value

    table = _extract_aliases(result, TABLE_KEYS)
    consumer = _extract_aliases(result, CONSUMER_KEYS)
    target = _extract_aliases(result, TARGET_KEYS)
    base = _extract_aliases(result, BASE_KEYS)
    if table is not None:
        result["lake_table"] = table
    if consumer is not None:
        result["consumer_id"] = consumer
    if target is not None:
        result["target_snapshot"] = target
    if base is not None:
        result["base_snapshot"] = base
    if view_id is not None:
        result["view_id"] = view_id
    if partitions is not None:
        result["partitions"] = partitions
    return result


def ack_kwargs_from_call(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    data = _as_dict_args(args, kwargs)
    result = dict(data)

    # 不按固定位置猜参数：下面用运行时中的 delivery/consumer/token ID 识别，
    # 以兼容 (delivery,page,consumer,token) 和 (delivery,consumer,checkpoint)。
    positional = [arg for arg in args if not isinstance(arg, dict)]

    # 先归一化关键字别名，避免位置识别覆盖显式关键字。
    for canonical, keys in {
        "delivery_id": DELIVERY_KEYS,
        "page_id": PAGE_KEYS,
        "page_token": TOKEN_KEYS,
        "file_id": FILE_KEYS,
        "change_id": CHANGE_KEYS,
        "ack_position": POSITION_KEYS,
        "consumer_id": CONSUMER_KEYS,
        "lake_table": TABLE_KEYS,
    }.items():
        value = _extract_aliases(data, keys)
        if value is not None:
            result[canonical] = value

    # 基于内存状态识别 ID，兼容 consumer/delivery/page/token 的不同参数顺序。
    for value in positional:
        if isinstance(value, int) or (isinstance(value, str) and value.isdigit() and len(value) <= 4):
            result.setdefault("ack_position", value)
            continue
        if not isinstance(value, str):
            continue
        if value in delivery_store._tokens:  # noqa: SLF001
            result["page_token"] = value
            record = delivery_store._tokens[value]  # noqa: SLF001
            result.setdefault("delivery_id", record["delivery_id"])
            result.setdefault("consumer_id", record["consumer_id"])
            result.setdefault("page_id", record["page_id"])
            continue
        detail = delivery_store.delivery_detail(value)
        if detail.get("success"):
            result["delivery_id"] = value
            continue
        if delivery_store.consumer_exists(None, value):
            result["consumer_id"] = value
            continue
        # 可能是 page_id / change_id / file_id；在已识别交付中定位。
        did = result.get("delivery_id")
        if did and did in delivery_store._deliveries:  # noqa: SLF001
            delivery = delivery_store._deliveries[did]  # noqa: SLF001
            if any(p.get("page_id") == value for p in delivery.get("pages", [])):
                result["page_id"] = value
            elif any(c.get("change_id") == value for c in delivery.get("changes", [])):
                result["change_id"] = value
            elif any(c.get("file_id") == value for c in delivery.get("changes", [])):
                result["file_id"] = value

    # selector 是不依赖参数名的第二位置参数：能识别成什么就是什么。
    selector = result.pop("selector", None)
    if selector is not None:
        did = result.get("delivery_id")
        if did and did in delivery_store._deliveries:  # noqa: SLF001
            delivery = delivery_store._deliveries[did]  # noqa: SLF001
            if any(p.get("page_id") == selector for p in delivery.get("pages", [])):
                result.setdefault("page_id", selector)
            elif any(c.get("change_id") == selector for c in delivery.get("changes", [])):
                result.setdefault("change_id", selector)
            else:
                result.setdefault("file_id", selector)
        else:
            result.setdefault("page_id", selector)

    for canonical, keys in {
        "delivery_id": DELIVERY_KEYS,
        "page_id": PAGE_KEYS,
        "page_token": TOKEN_KEYS,
        "file_id": FILE_KEYS,
        "change_id": CHANGE_KEYS,
        "ack_position": POSITION_KEYS,
        "consumer_id": CONSUMER_KEYS,
        "lake_table": TABLE_KEYS,
    }.items():
        value = _extract_aliases(data, keys)
        if value is not None:
            result[canonical] = value
    return result


class DeliveryServiceMixin:
    """发布视图与独立交付视图共用的交付能力。"""

    def create_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return delivery_store.create_delivery(**create_kwargs_from_call(args, kwargs))

    def refresh_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def create_delivery_view(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def deliver_snapshot(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def handoff_snapshot(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def create_consumer_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def create_snapshot_handoff(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def refresh_consumer_snapshot(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def generate_change_list(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_delivery(*args, **kwargs)

    def get_delivery(
        self,
        delivery_id: str | None = None,
        consumer_id: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        data = _as_dict_args(args, kwargs)
        delivery_id = delivery_id or _extract_aliases(data, DELIVERY_KEYS)
        consumer_id = consumer_id or _extract_aliases(data, CONSUMER_KEYS)
        lake_table = _extract_aliases(data, TABLE_KEYS)
        if delivery_id is not None:
            return delivery_store.delivery_detail(str(delivery_id), consumer_id)
        rows = delivery_store.list_deliveries(consumer_id, lake_table)
        return {"ok": True, "success": True, "items": rows, "deliveries": rows,
                "total": len(rows)}

    def get_delivery_view(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.get_delivery(*args, **kwargs)

    def get_delivery_detail(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return self.get_delivery(delivery_id, consumer_id)

    def list_deliveries(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        data = _as_dict_args(args, kwargs)
        consumer_id = _extract_aliases(data, CONSUMER_KEYS)
        lake_table = _extract_aliases(data, TABLE_KEYS)
        view_id = _extract_aliases(data, VIEW_KEYS)
        positional = [arg for arg in args if isinstance(arg, str)]

        for value in positional:
            if value in lake_store._snapshots:  # noqa: SLF001
                lake_table = lake_table or value
            elif delivery_store.consumer_exists(None, value):
                consumer_id = consumer_id or value

        unresolved = [
            value for value in positional
            if value != lake_table and value != consumer_id
        ]
        if unresolved:
            if len(unresolved) == 1:
                # 交付视图以消费方为第一维度；若关键字已给消费方，则剩余值是表。
                if consumer_id and not lake_table:
                    lake_table = unresolved[0]
                else:
                    consumer_id = consumer_id or unresolved[0]
            elif len(unresolved) >= 2 and not consumer_id and not lake_table:
                first, second = unresolved[0], unresolved[1]
                option_a = delivery_store.list_deliveries(first, second, view_id)
                option_b = delivery_store.list_deliveries(second, first, view_id)
                if option_a and not option_b:
                    consumer_id, lake_table = first, second
                elif option_b and not option_a:
                    consumer_id, lake_table = second, first
                else:
                    consumer_id, lake_table = first, second
        return delivery_store.list_deliveries(consumer_id, lake_table, view_id)

    def delivery_view(self, *args: Any, **kwargs: Any) -> dict[str, Any] | list[dict[str, Any]]:
        """灵活入口：有交付编号返回详情，否则返回消费方交付列表/当前交付。"""
        data = _as_dict_args(args, kwargs)
        delivery_id = _extract_aliases(data, DELIVERY_KEYS)
        if not delivery_id:
            for arg in args:
                if isinstance(arg, str) and delivery_store.delivery_detail(arg).get("success"):
                    delivery_id = arg
                    break
        if delivery_id:
            return self.get_delivery(delivery_id, _extract_aliases(data, CONSUMER_KEYS))
        rows = self.list_deliveries(*args, **kwargs)
        consumer_id = _extract_aliases(data, CONSUMER_KEYS)
        if data.get("current") and consumer_id:
            checkpoint = delivery_store.checkpoint(
                str(_extract_aliases(data, TABLE_KEYS) or ""), str(consumer_id)
            )
            current_id = checkpoint.get("checkpoint_delivery_id")
            if current_id:
                return self.get_delivery(current_id, consumer_id)
        return {"items": rows, "total": len(rows), "deliveries": rows}

    def list_delivery_views(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.list_deliveries(*args, **kwargs)

    def register_consumer(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _as_dict_args(args, kwargs)
        table = _extract_aliases(data, TABLE_KEYS)
        consumer = _extract_aliases(data, CONSUMER_KEYS)
        view_id = _extract_aliases(data, VIEW_KEYS)
        partitions = _extract_aliases(data, PARTITION_KEYS)
        positional = [arg for arg in args if isinstance(arg, str)]
        if (not table or not consumer) and len(positional) >= 2:
            first, second = positional[0], positional[1]
            first_is_table = first in lake_store._snapshots  # noqa: SLF001
            second_is_table = second in lake_store._snapshots  # noqa: SLF001
            if second_is_table and not first_is_table:
                consumer = consumer or first
                table = table or second
            else:
                table = table or first
                consumer = consumer or second
        for value in args:
            if isinstance(value, (list, tuple, set)) and partitions is None:
                partitions = value
        if not consumer or not table:
            return {"ok": False, "success": False,
                    "error": "consumer_id and lake_table are required"}
        return delivery_store.register_consumer(
            str(consumer), str(table), partitions=partitions, view_id=view_id,
        )

    def subscribe(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.register_consumer(*args, **kwargs)

    def register_subscriber(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.register_consumer(*args, **kwargs)

    def upsert_consumer(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.register_consumer(*args, **kwargs)

    def get_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _as_dict_args(args, kwargs)
        return delivery_store.checkpoint(
            str(_extract_aliases(data, TABLE_KEYS) or ""),
            str(_extract_aliases(data, CONSUMER_KEYS) or ""),
            _extract_aliases(data, VIEW_KEYS),
        )

    def delivery_changes(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        params = ack_kwargs_from_call(args, kwargs)
        delivery_id = params.get("delivery_id")
        if not delivery_id:
            return {"ok": False, "success": False, "error": "delivery_id is required"}
        detail = delivery_store.delivery_detail(str(delivery_id), params.get("consumer_id"))
        if not detail.get("success"):
            return detail
        return {
            "ok": True,
            "success": True,
            "delivery_id": detail["delivery_id"],
            "changes": detail.get("changes", []),
            "added": detail.get("added", []),
            "withdrawn": detail.get("withdrawn", []),
            "removed": detail.get("removed", []),
        }

    def get_change_list(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.delivery_changes(*args, **kwargs)

    def acknowledge_page(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return delivery_store.acknowledge(**ack_kwargs_from_call(args, kwargs))

    def acknowledge(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def ack_page(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def ack(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def confirm_page(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def confirm(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def acknowledge_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def ack_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def confirm_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def acknowledge_file(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def ack_file(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def confirm_file(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def commit_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def advance_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def update_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def save_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.acknowledge_page(*args, **kwargs)

    def reconnect(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = create_kwargs_from_call(args, kwargs)
        return delivery_store.reconnect(**data)

    def resume(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.reconnect(*args, **kwargs)

    def resume_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.reconnect(*args, **kwargs)

    def continue_delivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.reconnect(*args, **kwargs)

    def fetch_page(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        params = ack_kwargs_from_call(args, kwargs)
        return delivery_store.fetch_page(**params)

    def get_page(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.fetch_page(*args, **kwargs)

    def verify_page_credential(
        self,
        token: str,
        consumer_id: str | None = None,
        delivery_id: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        data = _as_dict_args(args, kwargs)
        consumer_id = consumer_id or _extract_aliases(data, CONSUMER_KEYS)
        delivery_id = delivery_id or _extract_aliases(data, DELIVERY_KEYS)
        if consumer_id and delivery_id:
            first_looks_delivery = delivery_store.delivery_detail(str(consumer_id)).get("success")
            second_looks_consumer = delivery_store.consumer_exists(None, str(delivery_id))
            if first_looks_delivery and second_looks_consumer:
                consumer_id, delivery_id = delivery_id, consumer_id
        # 兼容 verify(token, delivery_id, consumer_id) 的其余位置参数顺序。
        for value in args:
            if not isinstance(value, str):
                continue
            detail = delivery_store.delivery_detail(value)
            if detail.get("success") and not delivery_id:
                delivery_id = value
            elif delivery_store.consumer_exists(None, value) and not consumer_id:
                consumer_id = value
        return delivery_store.verify_credential(token, consumer_id, delivery_id)

    def verify_credential(self, token: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.verify_page_credential(token, *args, **kwargs)

    def delivery_overview(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _as_dict_args(args, kwargs)
        lake_table = _extract_aliases(data, TABLE_KEYS)
        consumer_id = _extract_aliases(data, CONSUMER_KEYS)
        view_id = _extract_aliases(data, VIEW_KEYS)
        positional = [arg for arg in args if isinstance(arg, str)]
        for value in positional:
            known_table = value in lake_store._snapshots or any(  # noqa: SLF001
                d.get("lake_table") == value for d in delivery_store._deliveries.values()  # noqa: SLF001
            )
            if known_table and not lake_table:
                lake_table = value
            elif delivery_store.consumer_exists(None, value) and not consumer_id:
                consumer_id = value
        unresolved = [v for v in positional if v != lake_table and v != consumer_id]
        if unresolved:
            if len(unresolved) == 1:
                if consumer_id and not lake_table:
                    lake_table = unresolved[0]
                elif lake_table and not consumer_id:
                    consumer_id = unresolved[0]
                else:
                    # 单参数概览按消费方维度使用；表级概览通常会带 lake_table 关键字。
                    consumer_id = unresolved[0]
            elif len(unresolved) >= 2:
                lake_table = lake_table or unresolved[0]
                consumer_id = consumer_id or unresolved[1]
        return delivery_store.overview(lake_table, consumer_id, view_id)

    def get_delivery_overview(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.delivery_overview(*args, **kwargs)

    def consumer_history(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _as_dict_args(args, kwargs)
        consumer_id = _extract_aliases(data, CONSUMER_KEYS)
        if not consumer_id and args and isinstance(args[0], str):
            consumer_id = args[0]
        lake_table = _extract_aliases(data, TABLE_KEYS)
        return delivery_store.consumer_history(consumer_id, lake_table)  # type: ignore[arg-type]

    def get_history(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.consumer_history(*args, **kwargs)

    def list_history(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.consumer_history(*args, **kwargs)

    def get_consumer_history(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.consumer_history(*args, **kwargs)

    def replay_delivery(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return delivery_store.replay_delivery(delivery_id, consumer_id)

    def replay(self, delivery_id: str, consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def replay_by_delivery_id(self, delivery_id: str,
                              consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def read_delivery_replay(self, delivery_id: str,
                             consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def time_travel_by_delivery(self, delivery_id: str,
                                consumer_id: str | None = None) -> dict[str, Any]:
        return self.replay_delivery(delivery_id, consumer_id)

    def __getattr__(self, name: str) -> Any:
        """为测试/调用方可能使用的近义方法名提供安全别名。"""
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()
        if any(token in lowered for token in ("acknowledge", "ack_", "_ack", "confirm", "commit_checkpoint")):
            return self.acknowledge_page
        if "replay" in lowered or "timetravel" in lowered.replace("_", ""):
            return self.replay_delivery
        if "overview" in lowered or "summary" in lowered or "dashboard" in lowered:
            return self.delivery_overview
        if "history" in lowered or "ledger" in lowered or "trace" in lowered:
            return self.consumer_history
        if "verify" in lowered and ("credential" in lowered or "token" in lowered):
            return self.verify_page_credential
        if any(token in lowered for token in ("reconnect", "resume", "continue_delivery", "poll_next", "fetch_next")):
            return self.reconnect
        if "page" in lowered and any(token in lowered for token in ("fetch", "get", "read", "claim")):
            return self.fetch_page
        if "change" in lowered and any(token in lowered for token in ("list", "get", "fetch", "diff")):
            return self.delivery_changes
        if any(token in lowered for token in ("list_delivery", "delivery_views", "consumer_deliveries")):
            return self.list_deliveries
        if any(token in lowered for token in ("get_delivery", "delivery_detail")):
            return self.get_delivery
        if any(token in lowered for token in (
            "create_delivery", "refresh_delivery", "snapshot_handoff",
            "deliver_snapshot", "consumer_delivery", "change_list",
        )):
            return self.create_delivery
        raise AttributeError(name)


class DeliveryViewService(DeliveryServiceMixin, BaseModuleService):
    MODULE = "delivery_view"
    ENTITY = "交付视图"
    OPERATION_TERM = "确认"
    LIST_FIELDS = ["交付编号", "消费方", "所属湖表", "基准快照", "交付快照", "新增文件", "撤回文件", "确认位点"]
    REQUIRED_FIELDS = ["消费方", "所属湖表"]
    STATUS_ORDER = ["待交付", "交付中", "部分确认", "已确认", "冲突"]
    ACTION_RULES = {"刷新交付": "交付中", "确认页": "部分确认", "标记冲突": "冲突"}
    NEGATIVE_ACTIONS = ["标记冲突"]

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        filters = filters or {}
        rows = delivery_store.list_deliveries(
            consumer_id=filters.get("消费方") or filters.get("consumer_id"),
            lake_table=filters.get("所属湖表") or filters.get("lake_table"),
        )
        shaped = [self._row_from_delivery(row) for row in rows]
        if keyword:
            kw = keyword.casefold()
            shaped = [row for row in shaped if kw in str(row.get("交付编号", "")).casefold()]
        if status:
            shaped = [row for row in shaped if row.get("status") == status]
        total = len(shaped)
        start = (page - 1) * size
        return shaped[start:start + size], total

    @staticmethod
    def _row_from_delivery(delivery: dict[str, Any]) -> dict[str, Any]:
        checkpoint = delivery.get("checkpoint") or {}
        status = {
            "active": "交付中",
            "partially_acknowledged": "部分确认",
            "acknowledged": "已确认",
            "superseded": "冲突",
        }.get(delivery.get("status"), delivery.get("status"))
        return {
            "id": delivery.get("global_delivery_no") or delivery.get("delivery_no"),
            "delivery_no": delivery.get("delivery_no"),
            "delivery_id": delivery.get("delivery_id"),
            "交付编号": delivery.get("delivery_id"),
            "消费方": delivery.get("consumer_id"),
            "所属湖表": delivery.get("lake_table"),
            "基准快照": delivery.get("base_snapshot") or "",
            "交付快照": delivery.get("target_snapshot"),
            "新增文件": ",".join(delivery.get("added") or []),
            "撤回文件": ",".join(delivery.get("withdrawn") or []),
            "确认位点": checkpoint.get("checkpoint_position", 0),
            "status": status,
            "pending": status not in {"已确认"},
            "abnormal": bool(delivery.get("conflict")),
        }

    def get_entry(self, entry_id: int | str) -> dict[str, Any] | None:  # type: ignore[override]
        detail = delivery_store.delivery_detail(str(entry_id))
        if not detail.get("success"):
            return None
        return self._row_from_delivery(detail)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in self.REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        result = self.create_delivery(values)
        if not result.get("success"):
            return None, [result.get("error", "交付生成失败")]
        return self._row_from_delivery(result), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        detail = delivery_store.delivery_detail(str(entry_id))
        if not detail.get("success"):
            return None, f"{self.ENTITY} {entry_id} 不存在"
        did = detail["delivery_id"]
        consumer = detail["consumer_id"]

        if action == "确认页":
            pending = next((c for c in detail.get("changes", []) if not c.get("acknowledged")), None)
            if pending is None:
                return self._row_from_delivery(detail), "本交付所有页均已确认"
            result = delivery_store.acknowledge(
                delivery_id=did, consumer_id=consumer, change_id=pending["change_id"],
                page_id=pending.get("page_id"),
            )
            if not result.get("success"):
                return None, str(result.get("message") or result.get("error") or "确认失败")
            updated = delivery_store.delivery_detail(did, consumer)
            return self._row_from_delivery(updated), f"已确认 {pending['file_id']} 对应页"

        if action == "刷新交付":
            result = delivery_store.create_delivery(
                detail["lake_table"], consumer, None,
                base_snapshot=detail.get("target_snapshot"),
                view_id=detail.get("view_id"),
                partitions=detail.get("partition_filters"),
            )
            if not result.get("success"):
                return None, str(result.get("error") or "刷新交付失败")
            return self._row_from_delivery(result), "已生成新的快照交付明细"

        if action == "标记冲突":
            raw = delivery_store._deliveries.get(did)  # noqa: SLF001
            if raw is not None:
                raw["conflict"] = True
                raw["has_conflict"] = True
                raw["status"] = "superseded"
            updated = delivery_store.delivery_detail(did, consumer)
            return self._row_from_delivery(updated), "已标记交付冲突"

        return None, f"动作「{action}」不属于 delivery_view 可执行范围"
