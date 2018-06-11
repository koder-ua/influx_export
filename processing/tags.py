import re
import collections
from pathlib import Path
from typing import Dict, Tuple, List, Iterator, Any, Set

import yaml
from dataclasses import dataclass, field

from .serie import Serie


DevTags = Dict[Tuple[str, str], Dict[str, str]]


@dataclass
class TagMatcher:
    metric_re: str
    target_fields: Tuple[str]
    tags: Dict[Tuple[str, ...], Dict[str, str]]
    _matched: Set[str] = field(init=False)
    _not_matched: Set[str] = field(init=False)
    _metric_rr: Any = field(init=False)

    def __post_init__(self):
        self._not_matched = set()
        self._matched = set()
        self._metric_rr = re.compile(self.metric_re)

    def new_tags(self, serie: Serie) -> Dict[str, str]:
        if serie.metric in self._not_matched:
            return {}
        if serie.metric not in self._matched:
            if self._metric_rr.match(serie.metric):
                self._matched.add(serie.metric)
            else:
                self._not_matched.add(serie.metric)
                return {}

        try:
            keys = map(serie.tags.__getitem__, self.target_fields)
            return self.tags.get(tuple(keys), {})
        except KeyError:
            return {}


def iter_mappings(cfg: Dict[str, Any], curr_names: Tuple[str, ...], curr_vals: Tuple[str, ...]) -> Iterator:
    for key in cfg:
        if '=' in key:
            break
    else:
        yield curr_names, curr_vals, cfg
        return

    for key, val in cfg.items():
        if '=' in key:
            fname, fval = key.split("=")
            assert isinstance(val, dict)
            assert fname not in curr_names
            yield from iter_mappings(val, curr_names + (fname,), curr_vals + (fval,))
        else:
            yield curr_names, curr_vals, {key: val}


def load_dev_tags(*paths: str) -> List[TagMatcher]:
    #  target_fields_names -> {target_fields_values -> new_tags}
    res: Dict[Tuple[str, ...], Dict[Tuple[str, ...], Dict[str, str]]] = \
        collections.defaultdict(lambda: collections.defaultdict(dict))

    for path in paths:
        for metric, map in yaml.load(Path(path).expanduser().open()).items():
            for fields, vals, tags in iter_mappings(map, tuple(), tuple()):
                sfields, svals = zip(*sorted(zip(fields, vals)))
                cval = res[(metric,) + sfields][svals]
                if set(cval).intersection(tags):
                    raise ValueError(f"Tag merge failed - different values found " +
                                     "for {fields}. Curr {cval} new {tags}")
                cval.update(tags)

    return [TagMatcher(metric, tuple(tag_names), dict(tags_map.items()))
            for (metric, *tag_names), tags_map in res.items()]
