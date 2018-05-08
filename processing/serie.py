from typing import Dict

import numpy
from dataclasses import dataclass


@dataclass
class Serie:
    name: str
    metric: str
    tags: Dict[str, str]
    times: numpy.ndarray
    vals: numpy.ndarray

    def __str__(self) -> str:
        return f"R(name='{self.name}', sz={len(self.times)})"
