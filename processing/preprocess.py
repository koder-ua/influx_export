import pprint
import sys
import threading
import collections
from queue import Queue
from pathlib import Path
from typing import (Any, List, Dict, Iterator, Iterable, Callable, Tuple)

import numpy

from . import tags
from .tags import TagMatcher, load_dev_tags
from .config import LoadConfig
from .serie import Serie, merge_unit_dev_host
from .storage import RAWStorage, open_storage
from .stages import make_diff, allign_times


stages_cpp_path = Path(tags.__file__).parent.parent / "stages_cpp"
sys.path.append(stages_cpp_path)
from stages_cpp import stages_cpp
assert sys.path[-1] == stages_cpp_path
sys.path.pop()
stages_cpp.init(stages_cpp_path)


differentiate = {
    "diskio_read_bytes",
    "diskio_write_bytes",
    "libvirt_domain_block_stats_read_bytes_total",
    "libvirt_domain_block_stats_read_requests_total",
    "libvirt_domain_block_stats_write_bytes_total",
    "libvirt_domain_block_stats_write_requests_total",
    "libvirt_domain_info_cpu_time_seconds_total",
    "net_bytes_recv",
    "net_bytes_sent",
}


def taggify_and_make_serie(old_serie: Serie,
                           tags: List[TagMatcher],
                           new_vals: numpy.ndarray,
                           tms: numpy.ndarray) -> Serie:
    new_attrs = serie_taggify_func(old_serie, tags)
    for k, v in new_attrs.items():
        assert old_serie.attrs.get(k) in (v, None, '')
    new_attrs.update(old_serie.attrs)
    return Serie(old_serie.name, old_serie.metric, attrs=new_attrs, vals=new_vals, origin=old_serie.vals, times=tms)


def expected_points(cfg: LoadConfig, tz_time_offset: int = 0) -> List[int]:
    res: List[int] = []
    curr = cfg.frm
    while curr <= cfg.to:
        res.append(int(curr.timestamp()) + tz_time_offset)
        curr = curr + cfg.step
    return res


def allign_diff(serie: Serie, expected_tms: List[int]) -> Tuple[numpy.ndarray, bool]:
    arr = allign_times(expected_tms, serie.vals, serie.times)
    if serie.metric in differentiate:
        return make_diff(arr), True
    return arr, False


def client_func(expected_tms: List[int], src: Iterator[Serie], tags: List[TagMatcher]) -> Iterator[Serie]:
    tms = numpy.ndarray(expected_tms, dtype=numpy.uint32)
    diff_tms = tms[1:]
    for serie in src:
        data, is_diff = allign_diff(serie, expected_tms)
        yield taggify_and_make_serie(serie, tags, data, diff_tms if is_diff else tms)


def serie_taggify_func(serie: Serie, tags: List[TagMatcher]) -> Dict[str, str]:
    res = {}
    for tagger in tags:
        ntags = tagger.new_tags(serie.metric, serie.attrs)
        if set(res).intersection(ntags):
            raise ValueError(f"Duplicated tags for {serie}: {res}, {ntags}")
        res.update(ntags)
    return res


TOP_PERCENT = 95
WINDOW_SIZE = 16


def job_func(it: Iterator[Serie], tms: numpy.ndarray) -> Iterator[Tuple[Serie, numpy.ndarray, bool]]:
    for serie in it:
        id_diff = serie.metric in differentiate
        data = stages_cpp.allign_diff(serie.times, serie.vals, tms, TOP_PERCENT, WINDOW_SIZE,
                                      diff=id_diff)
        yield serie, data, id_diff


def queue_to_iter(q: Queue) -> Iterator[Any]:
    while True:
        val = q.get()
        if val is None:
            break
        yield val


# can't use ThreadPool here, as it can't control memory consumption
# need pipeline with backpressure

def job_func_mt(source_iter: Iterator[Serie],
                job_count: int,
                tms: numpy.ndarray) -> Iterator[Tuple[Serie, numpy.ndarray, bool]]:

    def thmain(q1: Queue, q2: Queue, tms: numpy.ndarray):
        for serie, data, is_diff in job_func(queue_to_iter(q1), tms):
            q2.put((serie, data, is_diff))

    params_q = Queue()
    res_q = Queue()
    threads = [threading.Thread(target=thmain, args=(params_q, res_q, tms), daemon=True) for _ in range(job_count)]

    for th in threads:
        th.start()

    try:
        for i in range(job_count * 2):
            params_q.put(next(source_iter))

        while True:
            yield res_q.get()
            params_q.put(next(source_iter))
    except StopIteration:
        pass

    for i in range(job_count):
        params_q.put(None)

    for th in threads:
        th.join()

    while not res_q.empty():
        yield res_q.get()


def ctypes_client_func(expected_tms: List[int], src: Iterable[Serie], tags: List[TagMatcher], job_count: int) \
        -> Iterator[Serie]:

    tms = numpy.array(expected_tms, dtype=numpy.uint32)
    diff_tms = tms[1:]

    source_iter = iter(src)
    if job_count == 1:
        res_iter = job_func(source_iter, tms)
    else:
        res_iter = job_func_mt(source_iter, job_count, tms)

    for serie, data, is_diff in res_iter:
        yield taggify_and_make_serie(serie, tags, data, diff_tms if is_diff else tms)


def get_max_time_range(src: RAWStorage, filter: Callable[[Serie], bool] = None) -> Tuple[int, int]:
    begin = end = None
    for serie in src.iter_meta_only():
        if filter and not filter(serie):
            continue
        cbegin, cend = src.get_time_range(serie)
        if begin is None or cbegin < begin:
            begin = cbegin
        if end is None or cend > end:
            end = cend
    return begin, end


def load_tags(storage: Path, cluster: str) -> List[TagMatcher]:
    tags_root = storage / 'dev_tags'
    tags = [tags_root / (cluster + ".yaml"), *tags_root.glob(cluster + ".*.yaml")]
    return load_dev_tags(*map(str, tags))


def do_import(opts: Any, config: LoadConfig):
    expected_tms = expected_points(config, tz_time_offset=opts.timeshifts[opts.cluster])
    raw_path = Path(opts.raw_src).expanduser()
    storage = Path(opts.storage_path).expanduser()
    tags = load_tags(storage, opts.cluster)

    with RAWStorage.open(raw_path, "r") as src:
        total_recs = len(src)
        prev_perc = 0
        begin, end = get_max_time_range(src)
        begin_idx = expected_tms.index(begin)
        end_idx = expected_tms.index(end)

        expected_tms = expected_tms[begin_idx: end_idx + 1]

        with open_storage(storage) as dst:

            def series_iter() -> Iterator[Serie]:
                for serie in src:
                    serie.attrs = merge_unit_dev_host(serie.metric, serie.attrs)
                    yield serie

            if opts.no_ctypes:
                if opts.max_jobs != 1:
                    print("Warning: >1 jobs only worked with ctypes preprocessor")
                it = client_func(expected_tms, series_iter(), tags)
            else:
                it = ctypes_client_func(expected_tms, series_iter(), tags, opts.max_jobs)

            saver = dst.get_saver(opts.cluster, config.raw, f"{opts.cluster}.hdf5", compact=opts.compact)

            for idx, serie in enumerate(it):
                new_perc = idx * 100 // total_recs
                if new_perc >= prev_perc + 10:
                    print(f"{new_perc}% done")
                    prev_perc = new_perc
                saver(serie)


def symulate_import(opts: Any, config: LoadConfig):
    storage = Path(opts.storage_path).expanduser()
    tags = load_tags(storage, opts.cluster)
    expected_tms = expected_points(config, tz_time_offset=opts.timeshifts[opts.cluster])
    raw_path = Path(opts.raw_src).expanduser()

    def filter(src: RAWStorage) -> Iterator[Serie]:
        for serie in src.iter_meta_only():
            serie.attrs = merge_unit_dev_host(serie.metric, serie.attrs)
            if serie.host == 'ceph001' and serie.device == 'sdg' and serie.metric == 'diskio_write_bytes':
                src.load_data(serie)
                print(serie.attrs)
                yield serie

    with RAWStorage.open(raw_path, "r") as src:
        # min_vals = collections.defaultdict(lambda: 1e20)
        # max_vals = collections.defaultdict(lambda: -1000)
        for serie in ctypes_client_func(expected_tms, filter(src), tags, opts.max_jobs):
            print(serie.attrs)
            # assert serie.vals.min() >= 0
            # min_vals[serie.metric] = min(serie.vals.min(), min_vals[serie.metric])
            # max_vals[serie.metric] = max(serie.vals.max(), max_vals[serie.metric])
            # from matplotlib import pyplot
            # f, axarr = pyplot.subplots(2, sharex=True)
            # axarr[0].set_title(serie.name)
            # axarr[0].plot(serie.origin)
            # diff = numpy.clip(serie.vals, -5000, 10000)
            # axarr[1].plot(diff)
            # pyplot.show()
            # exit(1)

        # pprint.pprint(max_vals)
        # pprint.pprint(min_vals)