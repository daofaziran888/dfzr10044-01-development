"""消费方交付视图、页凭据、冲突对账和交付回放测试。"""
from __future__ import annotations

import time

import pytest

from app.delivery_store import delivery_store
from app.lake_store import PartitionFile, lake_store
from app.services.delivery_view import DeliveryViewService
from app.services.publish_view import PublishViewService
from app.services.time_travel import TimeTravelService


@pytest.fixture(autouse=True)
def reset_deliveries():
    delivery_store.reset()
    yield
    delivery_store.reset()


def add_file(table: str, fid: str, partition: str = "p1") -> None:
    lake_store._files.setdefault(table, {})[fid] = PartitionFile(  # noqa: SLF001
        fid, table, partition, 10, 100, f"manifest-{fid}", time.time()
    )


def build_s1_s2(table: str):
    for fid in ("A", "B", "C"):
        add_file(table, fid, "p1")
    add_file(table, "P2", "p2")
    s0 = lake_store.create_snapshot(table, [], None)
    s1 = lake_store.create_snapshot(table, ["A", "B", "P2"], s0.snapshot_id)
    s2 = lake_store.create_snapshot(table, ["A", "C", "P2"], s1.snapshot_id)
    for snap in (s0, s1, s2):
        lake_store.mark_snapshot_available(table, snap.snapshot_id)
    return s0, s1, s2


class TestDeliveryScenario:
    def test_ack_checkpoint_conflict_partition_isolation_recycle_and_replay(self):
        table = "delivery_scenario_full"
        consumer = "sub-a"
        other = "sub-b"
        s0, s1, s2 = build_s1_s2(table)
        svc = PublishViewService()

        d1 = svc.create_delivery(table, consumer, s1.snapshot_id, s0.snapshot_id,
                                 partitions=["p1"])
        assert d1["success"]
        assert d1["added"] == ["A", "B"]

        change_a = next(c for c in d1["changes"] if c["file_id"] == "A")
        ack_a = svc.acknowledge(d1["delivery_id"], change_a["page_id"], consumer,
                                change_a["page_token"])
        assert ack_a["success"]
        assert ack_a["checkpoint_position"] == 1

        # 不同分区的消费方看不到 p1 的 A/B。
        other_d1 = svc.create_delivery(table, other, s1.snapshot_id, s0.snapshot_id,
                                       partitions=["p2"])
        assert other_d1["added"] == ["P2"]
        assert other_d1["target_file_ids"] == ["P2"]
        other_d2 = svc.refresh_delivery(table, other, s2.snapshot_id, s1.snapshot_id,
                                        partitions=["p2"])
        assert other_d2["added"] == []
        assert other_d2["withdrawn"] == []

        d2 = svc.refresh_delivery(table, consumer, s2.snapshot_id, s1.snapshot_id,
                                  partitions=["p1"])
        assert d2["success"]
        assert d2["added"] == ["C"]
        assert d2["withdrawn"] == ["B"]
        assert d2["conflicts"][0]["file_id"] == "B"

        change_b = next(c for c in d1["changes"] if c["file_id"] == "B")
        stale_ack = svc.acknowledge(d1["delivery_id"], change_b["page_id"], consumer,
                                    change_b["page_token"])
        assert not stale_ack["success"]
        assert stale_ack["conflict"]
        assert stale_ack["status_code"] == 409

        # 对方不能使用本消费方的页凭据或交付编号确认。
        cross = svc.acknowledge(d2["delivery_id"], d2["changes"][0]["page_id"], other,
                                d2["changes"][0]["page_token"])
        assert not cross["success"]
        assert cross["status_code"] == 403

        reconnect = svc.reconnect(consumer_id=consumer, page_token=change_b["page_token"])
        assert reconnect["success"]
        assert reconnect["conflict"]
        assert reconnect["resumed_page"]["page_id"] == change_b["page_id"]
        assert reconnect["current_delivery"]["delivery_id"] == d2["delivery_id"]
        assert reconnect["pending_added_file_ids"] == ["C"]
        assert reconnect["pending_withdrawn_file_ids"] == ["B"]
        assert "A" not in reconnect["pending_file_ids"]

        for change in d2["changes"]:
            result = svc.acknowledge(d2["delivery_id"], change["page_id"], consumer,
                                     change["page_token"])
            assert result["success"], result

        overview = svc.delivery_overview(lake_table=table, consumer_id=consumer)
        view = overview["consumers"][0]
        assert [f["file_id"] for f in view["retained_files"]] == ["A", "C"]
        withdrawn = view["withdrawn_files"][0]
        assert withdrawn["file_id"] == "B"
        assert withdrawn["ack_position"] == 3
        assert withdrawn["ever_delivered"]

        history = svc.consumer_history(consumer, table)
        events = [(e["event_type"], e["file_id"]) for e in history["history"]["ledger"]] \
            if "history" in history and isinstance(history["history"], dict) else \
            [(e["event_type"], e["file_id"]) for e in history["histories"][0]["ledger"]]
        assert ("added_offered", "B") in events
        assert ("withdrawal_offered", "B") in events

        # B 物理回收后，S1 旧快照仍保留原始 A/B 清单。
        lake_store.recycle_files(table, ["B"])
        travel = TimeTravelService()
        old_read = travel.read_at_snapshot(table, s1.snapshot_id)
        assert old_read["success"]
        assert old_read["file_ids"] == ["A", "B", "P2"]
        recycled_b = next(f for f in old_read["files"] if f["file_id"] == "B")
        assert recycled_b["recycled"]

        replay = travel.compare_delivery_with_snapshot(d2["delivery_id"], consumer)
        assert replay["success"]
        assert replay["file_ids"] == ["A", "C"]
        assert replay["snapshot_file_ids"] == ["A", "C", "P2"]
        assert replay["partition_scoped_snapshot_file_ids"] == ["A", "C"]
        assert replay["match_snapshot"]
        assert replay["aligned"]
        marker = replay["withdrawals"][0]
        assert marker["file_id"] == "B"
        assert marker["ack_position"] == 3
        assert marker["recycled"]

        replay_d1 = travel.replay_delivery(d1["delivery_id"], consumer)
        assert replay_d1["file_ids"] == ["A"]

    def test_credential_must_match_delivery(self):
        table = "delivery_credential"
        consumer = "cred-consumer"
        _, s1, _ = build_s1_s2(table)
        svc = DeliveryViewService()
        d1 = svc.create_delivery(table, consumer, s1.snapshot_id, None)
        d2 = svc.create_delivery(table, consumer,
                                 lake_store.list_snapshots(table)[-1].snapshot_id,
                                 s1.snapshot_id)
        token_a = d1["changes"][0]["page_token"]
        verify = svc.verify_page_credential(token_a, consumer, d2["delivery_id"])
        assert not verify["valid"]
        assert verify["reason"] == "delivery_mismatch"

        wrong_consumer = svc.verify_page_credential(token_a, "someone-else", d1["delivery_id"])
        assert not wrong_consumer["valid"]
        assert wrong_consumer["reason"] == "consumer_mismatch"

    def test_publish_view_refresh_returns_changes(self):
        table = "publish_refresh_changes"
        _, s1, s2 = build_s1_s2(table)
        svc = PublishViewService()
        svc.publish_view(table, "SELECT *", [], "pub-consumer", snapshot_id=s1.snapshot_id)
        result = svc.refresh_view(table, s1.snapshot_id, "pub-consumer",
                                  target_snapshot=s2.snapshot_id)
        assert result["success"]
        assert result["added"] == ["C"]
        assert result["withdrawn"] == ["B"]
        assert result["delivery_id"]
