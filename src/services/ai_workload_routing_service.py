# -*- coding: utf-8 -*-
"""Describe how local workstation tasks and cloud AI tasks are split."""

from __future__ import annotations

from typing import Any, Dict, List

from src.config import get_config, get_configured_llm_models


class AIWorkloadRoutingService:
    """Build a small, user-facing routing summary for AI-related work."""

    def __init__(self, config: Any = None) -> None:
        self.config = config or get_config()

    def build_status(self) -> Dict[str, Any]:
        configured_models = self._configured_models()
        channels = self._configured_channels()
        cloud_enabled = bool(getattr(self.config, "ai_cloud_analysis_enabled", True)) and bool(configured_models or channels)
        local_model_default = bool(getattr(self.config, "ai_local_model_default_enabled", False))

        return {
            "mode": getattr(self.config, "ai_routing_mode", "cloud_analysis_local_workstation"),
            "cloud_analysis": {
                "enabled": cloud_enabled,
                "provider_count": len(channels),
                "providers": channels,
                "primary_model": self._primary_model(configured_models),
                "configured_models": configured_models,
                "role": "复杂分析、报告生成、问股解释、需要判断的任务",
            },
            "local_workstation": {
                "enabled": True,
                "role": "本地数据沉淀、回测计算、持仓复盘材料、日志缓存清理",
                "tasks": [
                    "保存持仓和自选行情",
                    "批量回测历史分析",
                    "生成每日持仓复盘",
                    "清理旧日志和临时缓存",
                ],
            },
            "local_model": {
                "default_enabled": local_model_default,
                "role": "默认不参与股票分析；只适合隐私草稿、离线摘要或低价值批处理",
            },
            "recommendation": self._recommendation(cloud_enabled, local_model_default),
        }

    def _configured_models(self) -> List[str]:
        models = get_configured_llm_models(getattr(self.config, "llm_model_list", []) or [])
        fallbacks = list(getattr(self.config, "litellm_fallback_models", []) or [])
        legacy = [getattr(self.config, "litellm_model", "")]
        if getattr(self.config, "openai_api_keys", None) or getattr(self.config, "openai_api_key", None):
            legacy.append(getattr(self.config, "openai_model", ""))
        if getattr(self.config, "gemini_api_keys", None) or getattr(self.config, "gemini_api_key", None):
            legacy.append(getattr(self.config, "gemini_model", ""))
        seen = set()
        result: List[str] = []
        for item in [*models, *fallbacks, *legacy]:
            model = str(item or "").strip()
            if not model or model in seen:
                continue
            seen.add(model)
            result.append(model)
        return result[:8]

    def _configured_channels(self) -> List[str]:
        channels = []
        for item in getattr(self.config, "llm_channels", []) or []:
            name = str((item or {}).get("name") or "").strip()
            if name:
                channels.append(name)

        if getattr(self.config, "deepseek_api_keys", None):
            channels.append("deepseek")
        if getattr(self.config, "openai_api_keys", None) or getattr(self.config, "openai_api_key", None):
            channels.append("openai")
        if getattr(self.config, "gemini_api_keys", None) or getattr(self.config, "gemini_api_key", None):
            channels.append("gemini")

        seen = set()
        return [name for name in channels if not (name in seen or seen.add(name))]

    @staticmethod
    def _primary_model(configured_models: List[str]) -> str:
        return configured_models[0] if configured_models else ""

    @staticmethod
    def _recommendation(cloud_enabled: bool, local_model_default: bool) -> str:
        if cloud_enabled and not local_model_default:
            return "当前分工合理：本机做长期数据和批处理，云端模型做高质量分析。"
        if cloud_enabled and local_model_default:
            return "云端模型已配置，但本地模型仍设为默认；建议只把本地模型用于低价值批处理。"
        return "暂未识别到可用云端模型；复杂分析会退化为规则和模板，建议补齐云端 API。"
