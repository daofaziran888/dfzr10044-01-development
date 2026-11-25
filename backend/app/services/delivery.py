"""交付服务的短名称兼容模块。"""
from __future__ import annotations

from app.services.delivery_view import DeliveryViewService


class DeliveryService(DeliveryViewService):
    MODULE = "delivery"
    ENTITY = "交付"
