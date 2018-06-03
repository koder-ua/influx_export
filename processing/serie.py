from typing import Dict, Optional

import numpy
from dataclasses import dataclass, field


@dataclass
class Serie:
    name: str
    metric: str
    tags: Dict[str, str]
    times: Optional[numpy.ndarray] = None
    vals: numpy.ndarray = field(default_factory=lambda: numpy.array([]))
    offset: Optional[int] = None
    size: Optional[int] = None

    def __str__(self) -> str:
        return f"Serie(name='{self.name}', sz={len(self.vals)})"
