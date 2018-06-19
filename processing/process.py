import abc
import collections
from pathlib import Path
from typing import Callable, Dict, Tuple, Iterable, NamedTuple, Any, Optional, Set

import numpy
import pandas
import seaborn as sns
from matplotlib import pyplot
from sklearn import linear_model

from .serie import Serie
from .interfaces import IDataStorage
from .storage import open_storage

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
        key = key_func(serie.metric, serie.attrs)
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
        return f"ceph_diskio_write_bytes::ceph_data::{root_name}"

    @staticmethod
    def ceph_j_wr(root_name: str = "default") -> str:
        return f"ceph_diskio_write_bytes::ceph_journal::{root_name}"

    @staticmethod
    def ceph_data_ioqd(root_name: str = "default") -> str:
        return f"ceph_diskio_iops_in_progress::ceph_data::{root_name}"

    @staticmethod
    def ceph_disk_usage(root_name: str = "default") -> str:
        return f"ceph_disk_usage::{root_name}"

    ceph_cpu_usage = "ceph_cpu_usage"
    vm_disk_write = "vm_disk_write"


def sum_reduce(v1: numpy.ndarray, v2: numpy.ndarray) -> numpy.ndarray:
    return v1 + v2


def sum_serie_func(serie: Serie, data: numpy.ndarray):
    data += serie.vals


def avg_func(data: numpy.ndarray, count: int):
    data /= count


class Aggregator:
    metric: str = None
    def __init__(self, stor: IDataStorage, hdf5_fname: str, cluster: str, recalculate: bool, **params) -> None:
        self.stor = stor
        self.hdf5_fname = hdf5_fname
        self.cluster = cluster
        self.recalculate = recalculate

        self.target_serie_name: Optional[str] = None
        self.attrs: Dict[str, str] = None
        self.selectors: Dict[str, str] = None
        self.start_time: Optional[int] = None
        self.stop_time: Optional[int] = None

        self.post_init(**params)

    def post_init(self, **params):
        pass

    def update_func(self, serie: Serie, data: Optional[numpy.ndarray]) -> Optional[numpy.ndarray]:
        if data is None:
            return serie.vals.copy()
        data += serie.vals
        return data

    def final_func(self, res: numpy.ndarray, count: int) -> numpy.ndarray:
        return res

    def select(self) -> Iterable[Serie]:
        return self.stor.select_series(self.cluster, self.metric, self.start_time, self.stop_time, **self.selectors)

    def process_data(self) -> Serie:
        if self.target_serie_name is not None:
            if self.recalculate:
                self.stor.remove(self.cluster, self.target_serie_name)
            else:
                try:
                    return self.stor.get(self.cluster, self.target_serie_name)
                except FileNotFoundError:
                    pass

        res: numpy.ndarray = None
        times: numpy.ndarray = None
        idx = 0
        for idx, serie in enumerate(self.select()):
            if times is None:
                times = serie.times
            else:
                assert (times == serie.times).all()
            res = self.update_func(serie, res)

        if res is None:
            raise FileNotFoundError(f"No data found for cluster={self.cluster} " +
                                    f"metric={self.metric} selectors={self.selectors}")

        res = self.final_func(res, idx)

        serie = Serie(name=self.target_serie_name,
                      metric=self.target_serie_name,
                      vals=res,
                      times=times,
                      attrs=self.attrs)

        if self.target_serie_name is not None:
            self.stor.get_saver(self.cluster, None, self.hdf5_fname)(serie)

        return serie


class CephAggregatorBase(Aggregator):
    def post_init(self, crush_root: str):
        self.attrs = {'dev_type': 'ceph_data',
                      'crush_root': 'default',
                      'host': '*',
                      'device': 'ceph_data/default',
                      'units': "B"}
        self.selectors = {'dev_type': 'ceph_data', 'crush_root': crush_root}


class CephDisksDataIo(CephAggregatorBase):
    metric = "diskio_write_bytes"

    def post_init(self, crush_root: str):
        super().post_init(crush_root)
        self.target_serie_name = Markers.ceph_data_wr(crush_root)


class CephDisksUsageAverage(CephAggregatorBase):
    metric = "disk_used_percent"

    def post_init(self, crush_root: str):
        super().post_init(crush_root)
        self.target_serie_name = Markers.ceph_disk_usage(crush_root)

    def final_func(self, res: numpy.ndarray, count: int) -> numpy.ndarray:
        return res / count


class CephDisksIOPSInProgress(CephAggregatorBase):
    metric = "diskio_iops_in_progress"

    def post_init(self, crush_root: str):
        super().post_init(crush_root)
        self.target_serie_name = Markers.ceph_data_ioqd(crush_root)


class CephCPUUsageUser(Aggregator):
    metric = "cpu_usage_user"

    def post_init(self, node_name_prefix: str = 'ceph'):
        self.attrs = {'host': '*', 'device': 'cpu', 'units': "S"}
        self.selectors = {"cpu": "cpu-total"}
        self.target_serie_name = Markers.ceph_cpu_usage
        self.node_name_prefix = node_name_prefix

    def update_func(self, serie: Serie, data: Optional[numpy.ndarray]) -> Optional[numpy.ndarray]:
        if serie.host.startswith(self.node_name_prefix):
            return super().update_func(serie, data)
        return data


class DiskBottleneck(Aggregator):
    metric = "diskio_iops_in_progress"
    def post_init(self):
        self.attrs = {}
        self.target_serie_name = None
        self.selectors = {'dev_type': 'ceph_journal'}

    def process_data(self) -> Serie:
        qd_cnt: Dict[int, Dict[str, int]] = collections.defaultdict(dict)
        qd_cnt_node: Dict[int, Dict[str, int]] = collections.defaultdict(lambda: collections.defaultdict(int))
        node_disks: Dict[str, Set[str]] = collections.defaultdict(set)
        one_len: int = None
        for serie in self.select():
            if one_len is None:
                one_len = len(serie.vals)

            key = f"{serie.host}::{serie.device}"
            for sz in (1, 4, 8, 16, 32, 64, 128, 256):
                count = (serie.vals >= sz).sum()
                if count > 0:
                    qd_cnt[sz][key] = count
                    qd_cnt_node[sz][serie.host] += count
                node_disks[serie.host].add(serie.device)

        for k, v in sorted(qd_cnt_node[16].items(), key=lambda x: x[1])[-20:][::-1]:
            print(f"{k} => {v}, {int(v / len(node_disks[k]) / one_len * 100)}%")

        return None


class CPUBottleneck(Aggregator):
    metric = "cpu_usage_user"
    def post_init(self):
        self.attrs = {}
        self.target_serie_name = None
        self.selectors = {"cpu": "cpu-total"}

    def process_data(self) -> Serie:
        cpu_usage: Dict[int, Dict[str, int]] = collections.defaultdict(dict)
        one_len: int = None
        for serie in self.select():
            if serie.host == 'ceph085':
                pyplot.hist(serie.vals, bins=20)
                pyplot.show()
                exit(0)
            if one_len is None:
                one_len = len(serie.vals)

            for sz in (10, 20, 40, 80):
                count = (serie.vals >= sz).sum()
                if count > 0:
                    cpu_usage[sz][serie.host] = count

        for k, v in sorted(cpu_usage[20].items(), key=lambda x: x[1])[-20:][::-1]:
            print(f"{k} => {v}, {int(v / one_len * 100)}%")

        return None


def net_bottleneck(src: IDataStorage, cluster: str, start_time: int = None, stop_time: int = None):
    metric = "net_bytes_sent"
    net_usage: Dict[int, Dict[str, int]] = collections.defaultdict(dict)
    one_len: int = None
    dtime = None
    for serie in src.select_series(cluster, metric, start_time, stop_time):
        if one_len is None:
            one_len = len(serie.vals)
            dtime = serie.times[1] - serie.times[0]

        # print(serie.device, numpy.percentile(serie.vals, [90]))

        for mbps in (10, 100, 800):
            count = (serie.vals >= mbps * dtime * (2 ** 20)).sum()
            if count > 0:
                net_usage[mbps][f"{serie.host}::{serie.device}"] = count

    for k, v in sorted(net_usage[100].items(), key=lambda x: x[1])[-20:][::-1]:
        print(f"{k} => {v}, {int(v / one_len * 100)}%")

    return None


def process(cmd: str, opts: Any):
    with open_storage(Path(opts.storage_path).expanduser()) as src:
        if cmd in ('plot_ceph_io', 'plot_ceph_usage', 'plot_ceph_qd', 'plot_ceph_cpu_user'):
            params = src, opts.cluster + ".hdf5", opts.cluster, opts.force_update
            if cmd == 'plot_ceph_io':
                serie = CephDisksDataIo(*params, crush_root=opts.crush_root).process_data()
            elif cmd == 'plot_ceph_usage':
                serie = CephDisksUsageAverage(*params, crush_root=opts.crush_root).process_data()
            elif cmd == 'plot_ceph_qd':
                serie = CephDisksIOPSInProgress(*params, crush_root=opts.crush_root).process_data()
            elif cmd == 'plot_ceph_cpu_user':
                serie = CephCPUUsageUser(*params, node_name_prefix="ceph").process_data()
            else:
                raise ValueError(f"Unknown option {cmd}")

            serie.pandas.plot()
            pyplot.show()
        else:
            params = src, opts.cluster + ".hdf5", opts.cluster

            if cmd == 'plot_ceph_cpu_per_mb':
                params += (opts.force_update,)
                serie = CephDisksDataIo(*params, crush_root=opts.crush_root).process_data()
                serie2 = CephCPUUsageUser(*params, node_name_prefix="ceph").process_data()

                ps1 = serie.pandas
                ps2 = serie2.pandas[1:]

                s2 = ps2.rolling(100).sum() / ps1.rolling(100).sum()
                s2.plot()
                pyplot.show()
            elif cmd == 'cpu_bottleneck':
                CPUBottleneck(*params).process_data()
            elif cmd == 'disk_qd_bottleneck':
                DiskBottleneck(*params).process_data()
            elif cmd == 'network_bottleneck':
                net_bottleneck(src, opts.cluster)
            else:
                # serie.vals = numpy.clip(serie2.vals[1:] * 1e6 / serie.vals, 0, 1e-2)
                # f, axarr = pyplot.subplots(2, sharex=True)
                # agg1.pandas.plot(ax=axarr[0])
                # agg2.pandas.plot(ax=axarr[1])
                # agg1.pandas.plot()
                # agg2.pandas.plot(secondary_y=True)
                raise RuntimeError(f"Not supported subcommand {cmd}")


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

