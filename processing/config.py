import re
import time
import datetime
from pathlib import Path
from typing import List, Dict

from dataclasses import dataclass


@dataclass
class Selector:
    name: str
    filter: str


@dataclass
class LoadConfig:
    frm: datetime.datetime
    to: datetime.datetime
    step: datetime.timedelta
    maxperselect: int
    maxpersecond: int
    selectors: List[Selector]


def to_datetime(val: str) -> datetime.datetime:
    ts = time.mktime(time.strptime(val, "%Y-%m-%dT%H:%M:%S"))
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)


def to_timedelta(val: str) -> datetime.timedelta:
    coef = {"s": 1, "m": 60, "h": 3600}[val[-1]]
    return datetime.timedelta(seconds=int(val[:-1]) * coef)


def parse_config(cfg: str) -> LoadConfig:
    params: Dict[str, str] = {}
    selectors: List[Selector] = []
    for line in Path(cfg).expanduser().open("r"):
        line = line.strip()
        if not line or line.startswith(";") or line.startswith("#"):
            continue
        rr = re.match("(?P<name>[a-zA-Z_][a-zA-Z_0-9]*)=(?P<value>.*)$", line)
        if rr:
            params[rr.group("name")] = rr.group("value")
        else:
            assert re.match("[+=]?[a-zA-Z_][a-zA-Z_0-9]* ", line)
            selectors.append(Selector(*line.split(" ", 1)))

    assert "from" in params
    assert "to" in params
    assert "step" in params

    return LoadConfig(frm=to_datetime(params['from']),
                      to=to_datetime(params['to']),
                      step=to_timedelta(params['step']),
                      maxperselect=int(params.get("maxperselect", "0")),
                      maxpersecond=int(params.get("maxpersecond", "0")),
                      selectors=selectors)
