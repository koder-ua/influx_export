from typing import Callable, Dict, Tuple, Iterable, NamedTuple, List

import numpy
import pandas
import seaborn as sns
from matplotlib import pyplot
from sklearn import linear_model

from .serie import Serie
from .storage import HDF5Storage

sns.set_style("darkgrid")

KeyFunc = Callable[[str, Dict[str, str]], Tuple[str, ...]]
ReduceFunc = Callable[[numpy.ndarray, numpy.ndarray], numpy.ndarray]


class Metrics:
    diskio_write_bytes = "diskio_write_bytes"
    vm_disk_write = "libvirt_domain_block_stats_write_bytes_total"
    disk_used_perc = "disk_used_percent"


def reduce_func(it: Iterable[Serie], key_func: KeyFunc, reduce_func: ReduceFunc) -> \
        Dict[Tuple[str, ...], Tuple[int, numpy.ndarray]]:

    grouped_reduced: Dict[Tuple[str, ...], Tuple[int, numpy.ndarray]] = {}

    for serie in it:
        key = key_func(serie.metric, serie.tags)
        try:
            cnt, curr  = grouped_reduced[key]
        except KeyError:
            grouped_reduced[key] = 1, serie.vals
        else:
            grouped_reduced[key] = cnt + 1, reduce_func(serie.vals, curr)

    return grouped_reduced


def plot(arrs: Iterable[Tuple[str, Serie]], clip: int = 1e6):

    for name, serie in arrs:
        pyplot.plot(serie.vals.clip(0, clip), label=name)

    pyplot.legend()
    pyplot.show()


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


class Markers:
    "Class contains markers for ts grooping and hdf5 storing"

    @staticmethod
    def ceph_data_wr(root_name: str = "default") -> str:
        return f"ceph/diskio_write_bytes::ceph_data::{root_name}"

    @staticmethod
    def ceph_j_wr(root_name: str = "default") -> str:
        return f"ceph/diskio_write_bytes::ceph_journal::{root_name}"

    @staticmethod
    def ceph_disk_usage(root_name: str = "default") -> str:
        return f"ceph/disk_usage::{root_name}"

    vm_disk_write = "vm/disk_write"


def sum_reduce(v1: numpy.ndarray, v2: numpy.ndarray) -> numpy.ndarray:
    return v1 + v2


def get_ceph_disks_data_io(stor: HDF5Storage, crush_root: str = 'default') -> Serie:
    target_serie = Markers.ceph_data_wr(crush_root)
    if target_serie in stor:
        return stor.get(target_serie)

    for idx, serie in enumerate(stor.select_series("raw", "diskio_write_bytes", dev_type='ceph_data', crush_root='default')):
        pass

    print("Total ceph io series count: ", idx)


# def process_ceph_disks(taggify_func: Callable[[Serie], Tuple[bool, Dict[str, str]]],
#                        expected_tms: List[int],
#                        needed_markers: List[str],
#                        input_storage: RAWStorage) -> Iterable[Serie]:
#
#     def key_func(metric: str, tags: Dict[str, str]) -> Tuple[bool, Tuple[str, ...]]:
#         marker: Optional[str] = None
#         if metric == Metrics.diskio_write_bytes:
#             if tags['dev_type'] == 'ceph_data':
#                 marker = Markers.ceph_data_wr(tags['crush_root'])
#             elif tags['dev_type'] == 'ceph_journal':
#                 marker = Markers.ceph_j_wr(tags['crush_root'])
#             else:
#                 raise ValueError(f"Unknown disk type {tags['dev_type']}")
#         elif metric == Metrics.vm_disk_write:
#             marker = Markers.vm_disk_write
#
#         if marker not in needed_markers:
#             return False, tuple()
#         return marker is None, (marker,)
#
#
# def process_disks_usage(taggify_func: Callable[[Serie], Tuple[bool, Dict[str, str]]],
#                         expected_tms: List[int],
#                         needed_markers: List[str],
#                         input_storage: RAWStorage) -> Iterable[Serie]:
#
#     def key_func(metric: str, tags: Dict[str, str]) -> Tuple[bool, Tuple[str, ...]]:
#         if metric == Metrics.disk_used_perc and tags['dev_type'] == 'ceph_data':
#             marker = Markers.ceph_disk_usage(tags['crush_root'])
#             if marker in needed_markers:
#                 return True, (marker,)
#         return False, tuple()
#
#     map_func = functools.partial(allign_diff, expected_tms=expected_tms)
#     res_mp = tag_filter_map_reduce(input_storage,
#                                    taggify_func=taggify_func,
#                                    key_func=key_func,
#                                    map_func=map_func,
#                                    reduce_func=sum_reduce)
#
#     for (marker, *rest_tags), (cnt, arr) in res_mp.items():
#         yield Serie(marker, marker, {}, vals=arr / cnt)
#
#
#
#
#     elif opts.subparser_name == 'analyze':
#         output_markers = [
#             Markers.ceph_data_wr('crush_root'),
#             Markers.ceph_j_wr('default'),
#             Markers.ceph_data_wr('crush_root'),
#             Markers.ceph_j_wr('default'),
#             Markers.vm_disk_write
#         ]
#
#         missing_markers: List[str] = []
#         arrs = []
#
#         x = arrs_mp[Markers.ceph_j_wr("default")].vals
#
#         labels = {
#             Markers.ceph_j_wr("default"): "journal_wr",
#             Markers.ceph_data_wr("default"): "data_wr",
#             Markers.vm_disk_write: "vm_wr"
#         }
#         xp, x_h, x_l = cut_outliers(x, 24 * 60, 0.75, 0.25)
#
#         y = arrs_mp[Markers.vm_disk_write].vals
#         yp, _, _ = cut_outliers(y, 24 * 60, 0.75, 0.25)
#
#         def print_lr(x, y):
#             lr_res = lr(x.reshape((-1, 1)), y)
#             print(f"coef={1.0 / lr_res.coefs[0]:.1f}, residues={lr_res.residules:.2e} " +
#                   f"score={lr_res.score:.3f} rel_diff={lr_res.diff:.2e}")
#
#         def calc_window_coefs(x:numpy.ndarray, y:numpy.ndarray,
#                               window_size: int) -> List[numpy.ndarray]:
#             coefs = []
#             for i in range(len(x) // window_size):
#                 xw = x[i * window_size: (i + 1) * window_size]
#                 yw = y[i * window_size: (i + 1) * window_size]
#                 lr_res = lr(xw.reshape((-1, 1)), yw)
#                 coefs.append(lr_res.coefs)
#             return coefs
#
#         def plot_lr_coefs(x:numpy.ndarray, y:numpy.ndarray, window_size: int):
#             coefs = calc_window_coefs(x, y, window_size)
#             rcoefs = numpy.array([1.0 / x[0] for x in coefs])
#             pyplot.plot(rcoefs.clip(0, 15))
#             pyplot.show()
#
#         def plot_outliers():
#             idx1 = (x > (x_h * 3 + numpy.mean(x))).nonzero()[0]
#             idx2 = (x > (x_h * 8 + numpy.mean(x))).nonzero()[0]
#
#             idx1 = numpy.array(list(set(idx1).difference(set(idx2))))
#             # pyplot.plot(idx1, x[idx1], marker='o', ls='', color='blue')
#             pyplot.plot(idx2, x[idx2], marker='o', ls='', color='red')
#
#             pyplot.plot(x_h, alpha=0.5)
#             pyplot.plot(x_l, alpha=0.5)
#             pyplot.plot(x, alpha=0.3)
#
#             pyplot.show()
#
#         plot_outliers()
#         # print_lr(x, y)
#         # print_lr(xp, yp)
#         # plot_lr_coefs(xp, yp, 24 * 60)
#     else:
#         arrs = process_disks_usage(taggify_func, expected_tms, hdf_path, opts.raw_src[0])
#         x = arrs["ceph/total_usage::default"][1].vals
#         pyplot.plot(x, alpha=0.5)
#         pyplot.show()

