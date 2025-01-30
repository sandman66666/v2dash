"""
Gauges package for analytics metrics and dashboard data.
Each gauge represents a specific metric that can be displayed on the dashboard.
"""

from typing import Dict, Any
import asyncio
import logging

logger = logging.getLogger(__name__)

class GaugeResult:
    """Base class for gauge results"""
    def __init__(self, value: int, label: str, description: str = ""):
        self.value = value
        self.label = label
        self.description = description

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "label": self.label,
            "description": self.description
        }