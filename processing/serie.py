from typing import Dict, Optional, Tuple

import numpy
from dataclasses import dataclass, field


@dataclass
class Serie:
    name: str
    metric: str
    tags: Dict[str, str]
    times: numpy.ndarray = field(default_factory=lambda: numpy.array([]))
    vals: numpy.ndarray = field(default_factory=lambda: numpy.array([]))
    offset: Optional[int] = None
    size: Optional[int] = None

    def __str__(self) -> str:
        return f"Serie(name='{self.name}', sz={len(self.times)})"

    def get_host_dev(self) -> Tuple[str, str]:
        return self.tags['host'], '/dev/' + self.tags['name']
