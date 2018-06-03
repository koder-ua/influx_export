import re
import os
import sys
import time
import fcntl
import json
import struct
import datetime
import argparse
import selectors
import functools
import collections
import multiprocessing
from pathlib import Path
from typing import (Any, List, Dict, Tuple, Iterable, Iterator, Optional, Callable, Set, NamedTuple)

import yaml
import numpy
import pandas
import seaborn as sns
from matplotlib import pyplot
from dataclasses import dataclass, field
from sklearn import linear_model

from .serie import Serie
from .storage import RAWStorage, HDF5Storage
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


TaggifyFunc = Callable[[Serie], Tuple[bool, Dict[str, str]]]
KeyFunc = Callable[[str, Dict[str, str]], Tuple[bool, Tuple[str, ...]]]
MapFunc = Callable[[Serie], numpy.ndarray]
ReduceFunc = Callable[[numpy.ndarray, numpy.ndarray], numpy.ndarray]


def tag_filter_map_reduce(storage: RAWStorage,
                          taggify_func: TaggifyFunc,
                          key_func: KeyFunc,
                          map_func: MapFunc,
                          reduce_func: ReduceFunc) -> \
        Dict[Tuple[str, ...], Tuple[int, numpy.ndarray]]:

    grouped_reduced: Dict[Tuple[str, ...], Tuple[int, numpy.ndarray]] = {}

    for idx, serie in enumerate(storage.iter_meta_only()):
        tags = serie.tags.copy()
        ok_serie, new_tags = taggify_func(serie)

        if ok_serie:
            tags.update(new_tags)
            ok_serie, key = key_func(serie.metric, tags)
            if ok_serie:
                storage.load_data(serie)
                val = map_func(serie)
                if key in grouped_reduced:
                    cnt, curr  = grouped_reduced[key]
                    grouped_reduced[key] = cnt + 1, reduce_func(val, curr)
                else:
                    grouped_reduced[key] = 1, val

    return grouped_reduced


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


def plot(arrs: Iterable[Tuple[str, Serie]], clip: int = 1e6):

    for name, serie in arrs:
        pyplot.plot(serie.vals.clip(0, clip), label=name)

    pyplot.legend()
    pyplot.show()


class Metrics:
    diskio_write_bytes = "diskio_write_bytes"
    vm_disk_write = "libvirt_domain_block_stats_write_bytes_total"
    disk_used_perc = "disk_used_percent"


def rolling_percentile(data: numpy.ndarray, window: int, perc: float) -> numpy.ndarray:
    serie = pandas.Series(data)
    res = serie.rolling(window=window, center=True).quantile(quantile=perc)
    res[:window // 2] = res[window // 2]
    end_coef = len(res) - window // 2
    res[end_coef:] = res[end_coef]
    return res.values


class LRRes(NamedTuple):
    coefs: Tuple[float]
    inercept: float
    residules: float
    score: float
    diff: float


def lr(x: numpy.ndarray, y: numpy.ndarray) -> LRRes:
    regr = linear_model.LinearRegression()
    regr.fit(x, y)
    score = regr.score(x, y)
    diff = numpy.abs(regr.predict(x) - y).sum() / y.sum()
    return LRRes(regr.coef_, regr.intercept_, regr._residues, score, diff)


def cut_outliers(x: numpy.ndarray, window: int,
                 perc_h: float = 0.75, perc_l: float = 0.25) -> \
                 Tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]:
    x_h = rolling_percentile(x, window, perc_h)
    x_l = rolling_percentile(x, window, perc_l)
    xp = numpy.maximum(x, x_l)
    return numpy.minimum(xp, x_h), x_h, x_l


def serie_taggify_func(serie: Serie, tags: List[TagMatcher]) -> Dict[str, str]:
    res = {}
    for tagger in tags:
        ntags = tagger.new_tags(serie)
        if set(res).intersection(ntags):
            raise ValueError(f"Duplicated tags for {serie}: {res}, {ntags}")
        res.update(ntags)
    return res


class Markers:
    "Class contains markers for ts grooping and hdf5 storing"

    @staticmethod
    def ceph_data_wr(self, root_name: str = "default") -> str:
        return f"ceph/diskio_write_bytes::ceph_data::{root_name}"

    @staticmethod
    def ceph_j_wr(self, root_name: str = "default") -> str:
        return f"ceph/diskio_write_bytes::ceph_journal::{root_name}"

    @staticmethod
    def ceph_disk_usage(self, root_name: str = "default") -> str:
        return f"ceph/disk_usage::{root_name}"

    vm_disk_write = "vm/disk_write"


def allign_diff(curr_serie: Serie, expected_tms: List[int]) -> numpy.ndarray:
    return make_diff(allign_times(expected_tms, curr_serie.vals, curr_serie.times))


def sum_reduce(v1: numpy.ndarray, v2: numpy.ndarray) -> numpy.ndarray:
    return v1 + v2


def process_ceph_disks(taggify_func: Callable[[Serie], Tuple[bool, Dict[str, str]]],
                       expected_tms: List[int],
                       needed_markers: List[str],
                       input_storage: RAWStorage) -> Iterable[Serie]:

    def key_func(metric: str, tags: Dict[str, str]) -> Tuple[bool, Tuple[str, ...]]:
        marker: Optional[str] = None
        if metric == Metrics.diskio_write_bytes:
            if tags['dev_type'] == 'ceph_data':
                marker = Markers.ceph_data_wr(tags['crush_root'])
            elif tags['dev_type'] == 'ceph_journal':
                marker = Markers.ceph_j_wr(tags['crush_root'])
            else:
                raise ValueError(f"Unknown disk type {tags['dev_type']}")
        elif metric == Metrics.vm_disk_write:
            marker = Markers.vm_disk_write

        if marker not in needed_markers:
            return False, tuple()
        return marker is None, (marker,)

    yield from process_data(key_func, taggify_func, expected_tms, input_storage)


def preprocess_data(taggify_func: Callable[[Serie], Tuple[bool, Dict[str, str]]],
                    map_func: Callable[[Serie], numpy.ndarray],
                    input_storage: RAWStorage,
                    output_storage: HDF5Storage):

    #

    res_mp = tag_filter_map_reduce(input_storage,
                                   taggify_func=taggify_func,
                                   key_func=key_func,
                                   map_func=map_func,
                                   reduce_func=sum_reduce)

    for (marker,), (_, arr) in res_mp.items():
        yield Serie(marker, marker, {}, vals=arr)


def process_disks_usage(taggify_func: Callable[[Serie], Tuple[bool, Dict[str, str]]],
                        expected_tms: List[int],
                        needed_markers: List[str],
                        input_storage: RAWStorage) -> Iterable[Serie]:

    def key_func(metric: str, tags: Dict[str, str]) -> Tuple[bool, Tuple[str, ...]]:
        if metric == Metrics.disk_used_perc and tags['dev_type'] == 'ceph_data':
            marker = Markers.ceph_disk_usage(tags['crush_root'])
            if marker in needed_markers:
                return True, (marker,)
        return False, tuple()

    map_func = functools.partial(allign_diff, expected_tms=expected_tms)
    res_mp = tag_filter_map_reduce(input_storage,
                                   taggify_func=taggify_func,
                                   key_func=key_func,
                                   map_func=map_func,
                                   reduce_func=sum_reduce)

    for (marker, *rest_tags), (cnt, arr) in res_mp.items():
        yield Serie(marker, marker, {}, vals=arr / cnt)


def client_func(start_idx: int, max_count: int, expected_tms: List[int],
                src: RAWStorage, tags: List[TagMatcher], fd: int,
                min_flush: int):

    ready_data = []
    data_sz = 0

    for idx, serie in enumerate(src.iter_meta_only(start_idx)):
        if idx == max_count:
            break

        src.load_data(serie)
        data = allign_diff(serie, expected_tms)
        new_tags = serie_taggify_func(serie, tags)
        buff = data.tobytes()

        new_tags['__name__'] = serie.name
        meta = json.dumps(new_tags).encode("utf8")
        sizes = struct.pack(">II", len(meta), len(buff))
        ready_data.append(sizes + meta + buff)
        data_sz += len(ready_data[-1])

        if data_sz >= min_flush:
            os.write(fd, b"".join(ready_data))
            ready_data = []
            data_sz = 0


def preprocess(opts: Any):
    assert len(opts.raw_src) == 1
    F_SETPIPE_SZ = 1031
    max_pipe_sz = min(1024 ** 2, int(open("/proc/sys/fs/pipe-max-size").read()))

    cfg = parse_config(opts.sync_config)
    tags = load_dev_tags(*opts.dev_classes)
    expected_tms = expected_points(cfg, tz_time_offset=opts.tz_offset)

    path = Path(opts.raw_src[0]).expanduser()
    max_jobs = min(opts.max_jobs, multiprocessing.cpu_count())
    print(f"max_pipe_sz = {max_pipe_sz}")
    with RAWStorage.open(path, "r") as src:
        sz = len(src)
        rec_per_job = max(sz // max_jobs, 32)
        offsets: List[Tuple[int, int]] = []
        start = 0
        while start < sz:
            end = min(sz, start + rec_per_job)
            if end + rec_per_job > sz:
                end = sz
            offsets.append((start, end))
            start = end

        processed_data: Dict[int, List[bytes]] = {}
        active_pipes = 0
        sel = selectors.DefaultSelector()
        print(f"Will start {len(offsets)} child processes")
        for idx, (start, stop) in enumerate(offsets):
            read_fd, write_fd = os.pipe()
            fcntl.fcntl(write_fd, F_SETPIPE_SZ, max_pipe_sz)
            if os.fork() == 0:
                try:
                    time.sleep(0.0)
                    os.close(read_fd)
                    os.sched_setaffinity(0, [idx])
                    client_func(start, stop - start, expected_tms, src, tags, write_fd,
                                max_pipe_sz // 2)
                finally:
                    os.close(write_fd)
                os._exit(0)
            else:
                os.close(write_fd)
                flag = fcntl.fcntl(read_fd, fcntl.F_GETFD)
                fcntl.fcntl(read_fd, fcntl.F_SETFL, flag | os.O_NONBLOCK)
                processed_data[read_fd] = []
                active_pipes += 1
                sel.register(read_fd, selectors.EVENT_READ)

        while active_pipes:
            for key, mask in sel.select():
                assert mask == selectors.EVENT_READ
                data = os.read(key.fileobj, max_pipe_sz)
                if len(data) == 0:
                    active_pipes -= 1
                    sel.unregister(key.fileobj)
                else:
                    processed_data[key.fileobj].append(data)

    for pipe, lst in processed_data.items():
        print(f"Final data size {sum(map(len, lst))}")

    # with cast(IStorage, make_storage("hdf5", str(hdf_path), mode="a")) as hdf:
    #     for mark in missing_markers:
    #         hdf.save(arrs_mp[mark])


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='subparser_name')
    preprocess_parser = subparsers.add_parser('preprocess', help='Preprocess raw data from cluster')
    preprocess_parser.add_argument('-d', '--dev-classes', metavar="FILE", nargs='+')
    preprocess_parser.add_argument('-j', '--max-jobs', metavar="JOBS", type=int, default=multiprocessing.cpu_count())
    preprocess_parser.add_argument('-z', '--tz-offset', type=int, default=7200,
                                   help="workaround for timezone shift problem. Second diffe beetween cluster " +
                                        "and current timezone")
    preprocess_parser.add_argument('-s', '--sync-config', metavar="SYNC_CONFIG_FILE")
    preprocess_parser.add_argument('--hdf', metavar="HDF_FILE")
    preprocess_parser.add_argument('raw_src', metavar='RAW_SRC', nargs='*')

    analyze_parser = subparsers.add_parser('analyze', help='Run analisys over data')
    parser.add_argument('-c', '--cfg',
                        default=str(Path(__file__).parent / "config.yaml"))
    analyze_parser.add_argument('hdf-storage', metavar="FILE")
    return parser.parse_args(args[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)

    if opts.subparser_name == 'preprocess':
        preprocess(opts)
    elif opts.subparser_name == 'analyze':
        output_markers = [
            Markers.ceph_data_wr('crush_root'),
            Markers.ceph_j_wr('default'),
            Markers.ceph_data_wr('crush_root'),
            Markers.ceph_j_wr('default'),
            Markers.vm_disk_write
        ]

        missing_markers: List[str] = []
        arrs = []

        x = arrs_mp[Markers.ceph_j_wr("default")].vals

        labels = {
            Markers.ceph_j_wr("default"): "journal_wr",
            Markers.ceph_data_wr("default"): "data_wr",
            Markers.vm_disk_write: "vm_wr"
        }
        xp, x_h, x_l = cut_outliers(x, 24 * 60, 0.75, 0.25)

        y = arrs_mp[Markers.vm_disk_write].vals
        yp, _, _ = cut_outliers(y, 24 * 60, 0.75, 0.25)

        def print_lr(x, y):
            lr_res = lr(x.reshape((-1, 1)), y)
            print(f"coef={1.0 / lr_res.coefs[0]:.1f}, residues={lr_res.residules:.2e} " +
                  f"score={lr_res.score:.3f} rel_diff={lr_res.diff:.2e}")

        def calc_window_coefs(x:numpy.ndarray, y:numpy.ndarray,
                              window_size: int) -> List[numpy.ndarray]:
            coefs = []
            for i in range(len(x) // window_size):
                xw = x[i * window_size: (i + 1) * window_size]
                yw = y[i * window_size: (i + 1) * window_size]
                lr_res = lr(xw.reshape((-1, 1)), yw)
                coefs.append(lr_res.coefs)
            return coefs

        def plot_lr_coefs(x:numpy.ndarray, y:numpy.ndarray, window_size: int):
            coefs = calc_window_coefs(x, y, window_size)
            rcoefs = numpy.array([1.0 / x[0] for x in coefs])
            pyplot.plot(rcoefs.clip(0, 15))
            pyplot.show()

        def plot_outliers():
            idx1 = (x > (x_h * 3 + numpy.mean(x))).nonzero()[0]
            idx2 = (x > (x_h * 8 + numpy.mean(x))).nonzero()[0]

            idx1 = numpy.array(list(set(idx1).difference(set(idx2))))
            # pyplot.plot(idx1, x[idx1], marker='o', ls='', color='blue')
            pyplot.plot(idx2, x[idx2], marker='o', ls='', color='red')

            pyplot.plot(x_h, alpha=0.5)
            pyplot.plot(x_l, alpha=0.5)
            pyplot.plot(x, alpha=0.3)

            pyplot.show()

        plot_outliers()
        # print_lr(x, y)
        # print_lr(xp, yp)
        # plot_lr_coefs(xp, yp, 24 * 60)
    else:
        arrs = process_disks_usage(taggify_func, expected_tms, hdf_path, opts.raw_src[0])
        x = arrs["ceph/total_usage::default"][1].vals
        pyplot.plot(x, alpha=0.5)
        pyplot.show()

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
