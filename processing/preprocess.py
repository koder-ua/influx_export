import sys
import threading
from queue import Queue
from pathlib import Path
from typing import (Any, List, Dict, Iterator, Optional)

import numpy

from . import tags
from .tags import TagMatcher
from .config import LoadConfig
from .serie import Serie
from .storage import RAWStorage, HDF5Storage
from .stages import make_diff, allign_times


stages_cpp_path = Path(tags.__file__).parent.parent / "stages_cpp"
sys.path.append(stages_cpp_path)
from stages_cpp import stages_cpp
assert sys.path[-1] == stages_cpp_path
sys.path.pop()
stages_cpp.init(stages_cpp_path)


hdf_names = {
    "cpu_usage_guest": "{host}::cpu_usage_guest",
    "cpu_usage_iowait": "{host}::cpu_usage_iowait",
    "cpu_usage_system": "{host}::cpu_usage_system",
    "cpu_usage_user": "{host}::cpu_usage_user",
    "disk_used_percent": "{host}::{device}::disk_used_percent",
    "diskio_iops_in_progress": "{host}::{name}::diskio_iops_in_progress",
    "diskio_read_bytes": "{host}::{name}::diskio_read_bytes",
    "diskio_write_bytes": "{host}::{name}::diskio_write_bytes",
    "libvirt_domain_block_stats_read_bytes_total": "{domain}::libvirt_domain_block_stats_read_bytes_total",
    "libvirt_domain_block_stats_read_requests_total": "{domain}::libvirt_domain_block_stats_read_requests_total",
    "libvirt_domain_block_stats_write_bytes_total": "{domain}::libvirt_domain_block_stats_write_bytes_total",
    "libvirt_domain_block_stats_write_requests_total": "{domain}::libvirt_domain_block_stats_write_requests_total",
    "libvirt_domain_info_cpu_time_seconds_total": "{domain}::libvirt_domain_info_cpu_time_seconds_total",
    "libvirt_domain_info_maximum_memory_bytes": "{domain}::libvirt_domain_info_maximum_memory_bytes",
    "libvirt_domain_info_memory_usage_bytes": "{domain}::libvirt_domain_info_memory_usage_bytes",
    "libvirt_domain_info_virtual_cpus": "{domain}::libvirt_domain_info_virtual_cpus",
    "net_bytes_recv": "{host}::{interface}::net_bytes_recv",
    "net_bytes_sent": "{host}::{interface}::net_bytes_sent",
}


def hdf_name(serie: Serie) -> str:
    return hdf_names[serie.metric].format(**serie.tags)


def expected_points(cfg: LoadConfig, tz_time_offset: int = 0) -> List[int]:
    res: List[int] = []
    curr = cfg.frm
    while curr <= cfg.to:
        res.append(int(curr.timestamp()) + tz_time_offset)
        curr = curr + cfg.step
    return res


def allign_diff(curr_serie: Serie, expected_tms: List[int]) -> numpy.ndarray:
    return make_diff(allign_times(expected_tms, curr_serie.vals, curr_serie.times))


def serie_taggify_func(serie: Serie, tags: List[TagMatcher]) -> Dict[str, str]:
    res = {}
    for tagger in tags:
        ntags = tagger.new_tags(serie)
        if set(res).intersection(ntags):
            raise ValueError(f"Duplicated tags for {serie}: {res}, {ntags}")
        res.update(ntags)
    return res


TOP_PERCENT = 95
WINDOW_SIZE = 16


def taggify_and_make_serie(old_serie: Serie, tags: List[TagMatcher], new_vals: numpy.ndarray) -> Serie:
    new_tags = serie_taggify_func(old_serie, tags)
    new_tags.update(old_serie.tags)
    return Serie(old_serie.name, old_serie.metric, new_tags, vals=new_vals)


def ctypes_client_func(expected_tms: List[int], src: RAWStorage, tags: List[TagMatcher], job_count: int) \
        -> Iterator[Serie]:

    numpy_expected_tms = numpy.array(expected_tms, dtype=numpy.uint32)

    def iter_series(data_src: RAWStorage) -> Iterator[Serie]:
        for idx, serie in enumerate(data_src.iter_meta_only()):
            src.load_data(serie)
            yield serie

    it = iter_series(src)

    if job_count == 1:
        for serie in it:
            data = stages_cpp.allign_diff(serie.times, serie.vals, numpy_expected_tms, TOP_PERCENT, WINDOW_SIZE)
            yield taggify_and_make_serie(serie, tags, data)
        return

    def thmain(q1: Queue, q2: Queue):
        while True:
            serie = q1.get()
            if serie is None:
                return
            res = stages_cpp.allign_diff(serie.times, serie.vals, numpy_expected_tms, TOP_PERCENT, WINDOW_SIZE)
            q2.put((serie, res))

    # can't use ThreadPool here, as it can't control memory consumption
    # need pipeline backpressure

    params_q = Queue()
    res_q = Queue()
    threads = [threading.Thread(target=thmain, args=(params_q, res_q), daemon=True) for _ in range(job_count)]

    for th in threads:
        th.start()

    try:
        for i in range(job_count * 2):
            params_q.put(next(it))

        while True:
            serie, data = res_q.get()
            yield taggify_and_make_serie(serie, tags, data)
            params_q.put(next(it))
    except StopIteration:
        pass

    for i in range(job_count):
        params_q.put(None)

    for th in threads:
        th.join()

    while not res_q.empty():
        serie, data = res_q.get()
        yield taggify_and_make_serie(serie, tags, data)


def client_func(expected_tms: List[int], src: RAWStorage, tags: List[TagMatcher]) -> Iterator[Serie]:
    for idx, serie in enumerate(src.iter_meta_only()):
        src.load_data(serie)
        data = allign_diff(serie, expected_tms)
        yield taggify_and_make_serie(serie, tags, data)


def preprocess(opts: Any, config: LoadConfig, tags: List[TagMatcher]):
    assert len(opts.raw_src) == 1
    expected_tms = expected_points(config, tz_time_offset=opts.tz_offset)

    raw_path = Path(opts.raw_src[0]).expanduser()
    hdf_path = Path(opts.hdf).expanduser()

    tms_serie_name = "times_{}_{}_{}".format(expected_tms[0], expected_tms[-1], expected_tms[1] - expected_tms[0])

    with RAWStorage.open(raw_path, "r") as src:
        with HDF5Storage.open(hdf_path, "a") as dst:
            if tms_serie_name not in dst.fd:
                dst.fd[tms_serie_name] = numpy.array(expected_tms)

            if opts.ctypes:
                it = ctypes_client_func(expected_tms, src, tags, opts.max_jobs)
            else:
                if opts.max_jobs != 1:
                    print("Warning: >1 jobs only worked with ctypes preprocessor, use --ctypes switch")
                it = client_func(expected_tms, src, tags)

            for serie in it:
                hname = hdf_name(serie)
                if hname not in dst:
                    dst.save('raw/' + hdf_name(serie), serie)
                else:
                    print(hname)

