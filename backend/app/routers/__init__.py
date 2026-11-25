"""业务路由汇总：交付专用路由先注册，其后是通用资源路由。"""
from __future__ import annotations

from app.routers import delivery, resource

ROUTERS = [delivery, resource]
