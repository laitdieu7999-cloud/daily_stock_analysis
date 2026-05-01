# -*- coding: utf-8 -*-
"""
Shared metadata constants for analysis requests.
"""

from __future__ import annotations


SELECTION_SOURCES: tuple[str, ...] = ("manual", "autocomplete", "import", "image", "portfolio")
SELECTION_SOURCE_PATTERN = "^(" + "|".join(SELECTION_SOURCES) + ")$"
