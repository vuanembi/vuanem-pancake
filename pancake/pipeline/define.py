from typing import Any
from dataclasses import dataclass


@dataclass
class Pancake_statistics:
    name: str
    transform: list[dict[str, Any]]
    schema: list[dict[str, Any]]