# -*- coding: utf-8 -*-
"""System overview response schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SystemOverviewFileInfo(BaseModel):
    key: str
    label: str
    path: str
    exists: bool
    file_count: int
    size_bytes: int
    size_label: str
    modified_at: Optional[str] = None


class SystemOverviewPriority(BaseModel):
    priority: str
    label: str
    notify_rule: str
    archive_rule: str
    status: str


class SystemOverviewModule(BaseModel):
    key: str
    name: str
    priority: str
    status: str
    notify_rule: str
    archive_path: str
    detail: str


class SystemOverviewRecommendation(BaseModel):
    level: str
    title: str
    description: str


class SystemOverviewServiceStatus(BaseModel):
    key: str
    name: str
    status: str
    detail: str
    pid: Optional[int] = None
    updated_at: Optional[str] = None


class SystemOverviewAlert(BaseModel):
    level: str
    title: str
    description: str


class SystemOverviewResponse(BaseModel):
    generated_at: str
    scheduler: Dict[str, Any] = Field(default_factory=dict)
    services: List[SystemOverviewServiceStatus] = Field(default_factory=list)
    data_warehouse: Dict[str, Any] = Field(default_factory=dict)
    alerts: List[SystemOverviewAlert] = Field(default_factory=list)
    priorities: List[SystemOverviewPriority]
    modules: List[SystemOverviewModule]
    files: List[SystemOverviewFileInfo]
    recommendations: List[SystemOverviewRecommendation]
