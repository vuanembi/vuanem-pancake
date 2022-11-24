from typing import Any
from dataclasses import dataclass


@dataclass
class PancakeStatistics:
    name: str
    endpoint: str
    transform: list[dict[str, Any]]
    schema: list[dict[str, Any]]