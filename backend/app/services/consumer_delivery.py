"""按消费方交付的服务别名；主实现见 delivery_view / delivery_store。"""
from __future__ import annotations

from app.services.delivery_view import DeliveryViewService


class ConsumerDeliveryService(DeliveryViewService):
    MODULE = "consumer_delivery"
    ENTITY = "消费方交付"


class ConsumerDeliveryViewService(DeliveryViewService):
    MODULE = "consumer_delivery_view"
    ENTITY = "消费方交付视图"
