import datetime
import re
import sys
import argparse
import time
from typing import Any, List, Dict, Tuple, Iterable, Iterator, Optional, Callable
from pathlib import Path
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import h5py

import yaml
import numpy
import seaborn as sns
from matplotlib import pyplot
from dataclasses import dataclass

from .serie import Serie
from .storage import make_storage, INonIndexableStorage
from .stages import make_diff, allign_times


DevTags = Dict[Tuple[str, str], Dict[str, str]]


sns.set_style("darkgrid")


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
            assert re.match("[a-zA-Z_][a-zA-Z_0-9]* ", line)
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


def expected_points(cfg: LoadConfig, tz_time_offset: int = 0) -> List[int]:
    res: List[int] = []
    curr = cfg.frm
    while curr <= cfg.to:
        res.append(int(curr.timestamp()) + tz_time_offset)
        curr = curr + cfg.step
    return res


def tag_filter_map_reduce(storage: INonIndexableStorage,
                          taggify_func: Callable[[Serie], Optional[Dict[str, str]]],
                          tag_filter: Callable[[Dict[str, str]], bool],
                          key_func: Callable[[Dict[str, str]], Tuple[str, ...]],
                          reduce_func: Callable[[Optional[numpy.ndarray], Serie], numpy.ndarray]) -> \
        Dict[Tuple[str, ...], numpy.ndarray]:

    grouped_reduced: Dict[Tuple[str, ...], numpy.ndarray] = {}

    for serie in storage.iter_meta_only():
        tags = taggify_func(serie)

        if tags is not None and tag_filter(tags):
            key = key_func(tags)
            storage.load_data(serie)
            grouped_reduced[key] = reduce_func(grouped_reduced.get(key), serie)

    return grouped_reduced


def load_dev_tags(*paths: str) -> DevTags:
    res: DevTags = {}
    for path in paths:
        for node, dev_map in yaml.load(open(path)).items():
            for dev, tags in dev_map.items():
                curr = res.setdefault((node, dev), {})
                for tag_name in set(curr).intersection(tags.keys()):
                    if tags[tag_name] != curr[tag_name]:
                        raise ValueError(f"Tag merge failed - different values found " +
                                         "for tag {tag_name} for node={node}, dev = {dev}")
                curr.update(tags)
    return res


def plot(arrs: Iterable[Tuple[str, numpy.ndarray]], clip: int = 1e6):

    for name, val in arrs:
        pyplot.plot(val.clip(0, clip), label=name)

    pyplot.legend()
    pyplot.show()


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        default=str(Path(__file__).parent / "config.yaml"))
    parser.add_argument('-l', '--load-config', metavar="PATH", required=True)
    parser.add_argument('-d', '--dev-classes', metavar="FILE", nargs='+')
    parser.add_argument('raw_src', metavar='RAW_SRC', nargs='*')
    return parser.parse_args(args[1:])


class TSTypes:
    ceph_disk_io = "ceph_disk_io"
    vm_disk_io = "vm_disk_io"


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    cfg = parse_config(opts.load_config)
    tags = load_dev_tags(*opts.dev_classes)
    expected_tms = expected_points(cfg, tz_time_offset=7200)

    assert len(opts.raw_src) == 1

    def serie_taggify_func(serie: Serie) -> Optional[Dict[str, str]]:

        ceph_host = re.match("ceph\\d+", serie.tags['host']) or \
            re.match("ssd\\d+", serie.tags['host'])

        # if serie.metric == 'diskio_write_bytes' and ceph_host:
        #     return tags.get(serie.get_host_dev())

        if serie.metric == 'libvirt_domain_block_stats_write_bytes_total':
            return {"type": TSTypes.vm_disk_io}

        return None

    def io_reduce_func(curr: Optional[numpy.ndarray], curr_serie: Serie) -> numpy.ndarray:
        diff = make_diff(allign_times(expected_tms, curr_serie.vals, curr_serie.times))
        if curr is None:
            return diff
        diff += curr
        return diff

    def tag_key_func(tags: Dict[str, str]) -> Tuple[str, ...]:
        if tags['type'] == TSTypes.ceph_disk_io:
            return TSTypes.ceph_disk_io, tags['crush_root'], tags['ceph_disk_type']
        assert tags == {"type": TSTypes.vm_disk_io}
        return TSTypes.vm_disk_io,

    dsets = {
        "ceph_disk/default::journal": "journal",
        "ceph_disk/default::data": "data",
        "vm/disk_io": "vm_io"
    }

    arrs: Dict[str, Tuple[str, numpy.ndarray]] = {}

    with make_storage("raw", opts.raw_src[0]) as src:
        with h5py.File('/tmp/data.hdf') as hfd:
            all_found = True

            for name in dsets:
                grp, _ = name.split("/", 1)
                if grp not in hfd:
                    hfd.create_group(grp)

            for dname, label in dsets.items():
                if dname in hfd:
                    if dname == "vm/disk_io":
                        arrs[dname] = label, hfd[dname].value * 3
                    else:
                        arrs[dname] = label, hfd[dname].value
                else:
                    all_found = False
                    break

            if not all_found:
                res_mp = tag_filter_map_reduce(src,
                    taggify_func=serie_taggify_func,
                    tag_filter=lambda tags: tags.get('type') in (TSTypes.ceph_disk_io, TSTypes.vm_disk_io),
                    key_func=tag_key_func,
                    reduce_func=io_reduce_func)

                for (tp, *rest_tags), arr in res_mp.items():
                    if tp == TSTypes.ceph_disk_io:
                        root, dev_type = rest_tags
                        if root == 'default':
                            dname = f"ceph_disk/{root}::{tp}"
                            arrs[dname] = dsets[dname], arr
                            hfd.create_dataset(dname, data=arr)
                    elif tp == TSTypes.vm_disk_io:
                        assert len(rest_tags) == 0
                        dname = "vm/disk_io"
                        arrs[dname] = dsets[dname], arr
                        hfd.create_dataset(dname, data=arr)

    # diff = arrs["ceph_disk/default::data"][1] - arrs["ceph_disk/default::journal"][1]
    # plot([("dd", diff)] + list(arrs.values()))
    plot(arrs.values())
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))