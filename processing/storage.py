import mmap
import json
import struct
import sqlite3
import warnings
import datetime
from typing import Iterable, Tuple, Dict, Any, Iterator, Optional, Callable, Set
from pathlib import Path

import numpy

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import h5py

from .serie import Serie
from .interfaces import IDataStorage, IIndex


class RAWStorage:
    jan_first_2017 = int(datetime.datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp())

    time_format = "I"
    time_type_numpy = numpy.dtype(numpy.uint32).newbyteorder(">")
    time_sz = struct.calcsize(time_format)
    data_format = "Q"
    data_type_numpy = numpy.dtype(numpy.uint64).newbyteorder(">")
    data_sz = struct.calcsize(data_format)
    rec_sz = time_sz + data_sz

    def __init__(self, path: Path, fd: Any, mfd: mmap.mmap) -> None:
        self.path = path
        self.fd = fd
        self.mfd = mfd

    @classmethod
    def open(cls, name: str, mode: str = 'r') -> 'RAWStorage':
        path = Path(name).expanduser()
        fd = path.open(mode)
        mfd = mmap.mmap(fd.fileno(), 0, access=mmap.ACCESS_READ)
        return cls(path, fd, mfd)

    def __enter__(self) -> 'RAWStorage':
        assert self.mfd is not None
        return self

    def __exit__(self, type, value, traceback) -> bool:
        self.mfd.close()
        self.fd.close()
        self.mfd = None
        self.fd = None
        return False

    @staticmethod
    def split_serie_name(name: str) -> Tuple[str, Dict[str, str]]:
        metric, *str_tags = name.split(',')
        return metric, dict(str_tag.split("=", 1) for str_tag in str_tags)

    def get_time_range(self, serie: Serie) -> Tuple[int, int]:
        assert serie.offset is not None and serie.size is not None
        offset = serie.offset + self.data_sz * serie.size
        ts_begin, = struct.unpack(f">{self.time_format}", self.mfd[offset: offset + self.time_sz])
        offset += self.time_sz * (serie.size - 1)
        ts_end, = struct.unpack(f">{self.time_format}", self.mfd[offset: offset + self.time_sz])
        return ts_begin + self.jan_first_2017, ts_end + self.jan_first_2017

    def unpack_data(self, offset: int, sz: int) -> Tuple[numpy.ndarray, numpy.ndarray]:
        data = numpy.frombuffer(self.mfd[offset: offset + self.data_sz * sz], dtype=self.data_type_numpy)
        offset += self.data_sz * sz

        ts = numpy.frombuffer(self.mfd[offset: offset + self.time_sz * sz], dtype=self.time_type_numpy)
        ts = ts.astype(numpy.uint32)
        ts += self.jan_first_2017

        return ts, data.astype(numpy.float64)

    def _iter_recs_offsets(self, start_idx: int = 0) -> Iterable[Tuple[int, int, int]]:
        offset = 0
        cnt = 0
        while offset < len(self.mfd):
            name_end_offset = self.mfd.find(b'\x00', offset)
            data_offset = name_end_offset + 5  # '\x00' + 4b size
            sz, = struct.unpack(">I", self.mfd[data_offset - 4: data_offset])
            if cnt >= start_idx:
                # print(os.getpid(), offset, name_end_offset, data_offset, sz, self.mfd[offset: name_end_offset])
                yield offset, name_end_offset, data_offset, sz
            cnt += 1
            offset = data_offset + self.rec_sz * sz

    def iter_meta_only(self, start_idx: int = 0) -> Iterable[Serie]:
        for name_offset, name_end_offset, data_offset, sz in self._iter_recs_offsets(start_idx):
            name = self.mfd[name_offset: name_end_offset].decode("ascii")
            metric, tags = self.split_serie_name(name)
            yield Serie(name, metric, tags, offset=data_offset, size=sz)

    def load_data(self, serie: Serie):
        assert serie.offset is not None
        serie.times, data = self.unpack_data(serie.offset, serie.size)
        serie.vals = data / 1000

    def __iter__(self) -> Iterable[Serie]:
        for serie in self.iter_meta_only():
            self.load_data(serie)
            yield serie

    def __len__(self) -> int:
        idx = 0
        for idx, _ in enumerate(self._iter_recs_offsets()):
            pass
        return idx


class _NoVal: pass


class SQLiteIndex(IIndex):
    create_index_db = """create table IF NOT EXISTS series_index (
                            cluster TEXT NOT NULL,
                            name TEXT NOT NULL,
                            metric TEXT NOT NULL,
                            attrs TEXT NOT NULL,
                            config_id INTEGER,

                            start_time INT NOT NULL,
                            stop_time INT NOT NULL,

                            hdf5_file_name TEXT NOT NULL,
                            hdf5_name TEXT NOT NULL,
                            hdf5_ts_name TEXT NOT NULL)"""
    uniq_index_sql = "create unique index if not exists cluster_name on series_index (cluster, name)"
    create_configs_db = \
        "create table IF NOT EXISTS configs (id INTEGER PRIMARY KEY AUTOINCREMENT, config TEXT NOT NULL)"
    insert_serie_sql: str = "insert into series_index values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    select_serie_sql = "select * from series_index where metric=? and cluster=?"
    select_serie_by_name_sql = "select attrs, config_id, hdf5_file_name, hdf5_name, hdf5_ts_name " +\
        "from series_index where name=? and cluster=?"
    select_config_sql = "select id from configs where config=?"
    insert_config_sql = "insert into configs (config) values (?)"
    remove_rec_sql = "delete from series_index where cluster=? and name=?"

    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(str(path))
        self.cursor = self.db.cursor()
        self.cursor.execute(self.create_index_db)
        self.cursor.execute(self.uniq_index_sql)
        self.cursor.execute(self.create_configs_db)

    def close(self):
        self.cursor.close()
        self.db.close()

    def remove(self, cluster: str, name: str):
        self.cursor.execute(self.remove_rec_sql, (cluster, name))

    def insert_serie(self,
                     cluster: str,
                     hdf5_file: str,
                     hdf5_name: str,
                     hdf5_ts_name: str,
                     serie: Serie,
                     config_id: int = None):

        assert serie.times is not None
        self.cursor.execute(self.insert_serie_sql,
                            (cluster,
                             serie.name,
                             serie.metric,
                             json.dumps(serie.attrs),
                             config_id,

                             serie.times[0],
                             serie.times[-1],

                             hdf5_file,
                             hdf5_name,
                             hdf5_ts_name))

    def insert_config(self, cfg: str) -> int:
        ids = self.cursor.execute(self.select_config_sql, (cfg,)).fetchmany(2)
        assert len(ids) < 2

        if ids:
            return ids[0][0]

        self.cursor.execute(self.insert_config_sql, (cfg,))
        return self.cursor.lastrowid

    def sync(self):
        self.db.commit()

    def select_serie_by_name(self, cluster: str, name: str) -> Tuple[Dict[str, str], Optional[int], str, str, str]:
        res = self.cursor.execute(self.select_serie_by_name_sql, (name, cluster)).fetchmany(2)
        assert len(res) < 2
        if len(res) == 0:
            raise FileNotFoundError()
        attrs_js, config_id, hdf5_file_name, hdf5_name, hdf5_ts_name = res[0]
        return json.loads(attrs_js), config_id, hdf5_file_name, hdf5_name, hdf5_ts_name

    def select_series(self,
                      cluster: str,
                      metric: str,
                      start_time: Optional[int],
                      stop_time: Optional[int],
                      **selectors: str) -> Iterator[Tuple[str, Dict[str, str], Optional[int], int, int, str, str, str]]:

        sql = self.select_serie_sql
        ops = (metric, cluster)
        if start_time is not None:
            sql += " and stop_time > ?"
            ops += (start_time,)

        if stop_time is not None:
            sql += " and start_time < ?"
            ops += (stop_time,)

        it = self.cursor.execute(sql, ops)
        for _, name, _, attrs_js, *extra in it:
            attrs = json.loads(attrs_js)
            for k, v in selectors.items():
                if attrs.get(k, _NoVal) != v:
                    break
            else:
                yield (name, attrs, *extra)


def compact_data(data: numpy.ndarray, ttype: numpy.dtype) -> Tuple[numpy.ndarray, float, float]:
    assert str(ttype) in ('uint8', 'uint16', 'uint32')
    assert data.dtype.name == 'float64'
    dmin = data.min()
    res = numpy.log2(data - (dmin - 1))
    dmax = float(res.max())
    if dmax > 1E-8:
        res /= dmax / (256 ** ttype.itemsize - 1)
    return res.astype(ttype), dmin, dmax


def decompact_data(data: numpy.ndarray, dmin: float, dmax: float, ttype: numpy.dtype) -> numpy.ndarray:
    res = data.astype(ttype)
    if dmax > 1E-8:
        res *= dmax / (256 ** data.dtype.itemsize - 1)
    return numpy.exp2(res) + (dmin - 1)


class HDF5Storage:
    def __init__(self, path: Path, fd: h5py.File) -> None:
        self.path = path
        self.fd = fd
        self.times_cache: Dict = {}

    @classmethod
    def open(cls, file_name: str, mode: str = 'r') -> 'HDF5Storage':
        path = Path(file_name).expanduser()
        fd = h5py.File(str(path), mode)
        return cls(path, fd)

    def close(self):
        self.fd.close()

    def remove(self, hdf5_name: str):
        _, name, dst = self.ensure_hdf5_dir(hdf5_name)
        del dst[name]

    def ensure_hdf5_dir(self, full_hdf5_name: str) -> Tuple[Optional[str], str, h5py.Group]:
        if '/' in full_hdf5_name:
            assert full_hdf5_name.count('/') == 1
            hdf5_dir, name = full_hdf5_name.split('/')
            if hdf5_dir not in self.fd:
                self.fd.create_group(hdf5_dir)
            return hdf5_dir, name, self.fd[hdf5_dir]
        else:
            return None, full_hdf5_name, self.fd

    def save_times(self, hdf5_path: str, times: numpy.ndarray):
        _, ts_name, ts_dst = self.ensure_hdf5_dir(hdf5_path)
        try:
            ts_dst.create_dataset(ts_name, data=times)
        except RuntimeError:
            if ts_name in ts_dst:
                dset = ts_dst[ts_name]
                err = f"Times array {hdf5_path} already exists, but differ from current"
                assert dset.dtype == times.dtype, err
                assert dset.shape == times.shape, err
                assert all(dset.value == times), err
            else:
                raise
        self.times_cache[hdf5_path] = times.copy()

    def save(self, hdf5_name: str, serie: Serie, compact: bool = False) -> bool:
        _, name, dst = self.ensure_hdf5_dir(hdf5_name)
        if compact and serie.compaction_type:
            assert serie.compaction_type in (numpy.uint32, numpy.uint16, numpy.uint8)
            cdata, param1, param2 = compact_data(serie.vals, serie.compaction_type)
            compacted = True
        else:
            compacted = False
            cdata = serie.vals
            param1 = param2 = None

        try:
            dset = dst.create_dataset(name, data=cdata)
        except RuntimeError:
            if name in dst:
                print(f"Duplicated serie {name}")
                return False
            raise

        if compacted:
            dset.attrs['param1'] = param1
            dset.attrs['param2'] = param2
            dset.attrs['ttype'] = str(serie.vals.dtype)
        return True

    def __contains__(self, hdf5_name: str) -> bool:
        return hdf5_name in self.fd

    def get(self, hdf5_name: str, hdf5_ts_name: str, name: str, attrs: Dict[str, str]) -> Serie:

        data_dset = self.fd[hdf5_name]

        vals_arr = numpy.empty(data_dset.shape, dtype=data_dset.dtype)
        data_dset.read_direct(vals_arr)

        if 'param1' in data_dset.attrs:
            param1 = data_dset.attrs['param1']
            param2 = data_dset.attrs['param2']
            ttype = numpy.dtype(data_dset.attrs['ttype'])
            vals_arr = decompact_data(vals_arr, param1, param2, ttype)

        if hdf5_ts_name not in self.times_cache:
            times_arr = numpy.empty(data_dset.shape, dtype=data_dset.dtype)
            self.fd[hdf5_ts_name].read_direct(times_arr)
            self.times_cache[hdf5_ts_name] = times_arr.copy()
        else:
            times_arr = self.times_cache[hdf5_ts_name].copy()

        metric, _ = RAWStorage.split_serie_name(name)
        return Serie(name=name,
                     metric=metric,
                     attrs=attrs,
                     times=times_arr,
                     vals=vals_arr)


class HDF5AndSqliteDataStorage(IDataStorage):
    def __init__(self, sqlite_db_path: Path, hdf5_dir: Path) -> None:
        self.sql = SQLiteIndex(sqlite_db_path.expanduser())
        self.hdf5_dir = hdf5_dir.expanduser()
        self.config_ids: Dict[str, int] = {}
        self.existing_ts_hdf5_names: Set[str] = set()
        self.hdf5_files: Dict[str, HDF5Storage] = {}

    def __enter__(self) -> 'HDF5AndSqliteDataStorage':
        return self

    def __exit__(self, type, value, traceback):
        for fd in self.hdf5_files.values():
            fd.close()
        self.hdf5_files = {}
        self.sql.sync()
        self.sql.close()

    def get_hdf5_name(self, serie: Serie) -> str:
        if serie.times is not None:
            return f"{serie.host}::{serie.device}::{serie.metric}::{serie.times[0]}"
        return f"{serie.host}::{serie.device}::{serie.metric}"

    def get_hdf5_ts_name(self, serie: Serie) -> str:
        assert serie.times is not None and len(serie.times) > 1
        return f"__ts__::{serie.times[0]}::{serie.times[-1]}::{serie.times[-1] - serie.times[0]}"

    def get_hdf5_file(self, hdf5_fname: str) -> HDF5Storage:
        try:
            return self.hdf5_files[hdf5_fname]
        except KeyError:
            fd = self.hdf5_files[hdf5_fname] = HDF5Storage.open(self.hdf5_dir / hdf5_fname, 'a')
            return fd

    def get(self, cluster: str, name: str) -> Serie:
        attrs, config_id, hdf5_fname, hdf5_name, hdf5_ts_name = self.sql.select_serie_by_name(cluster, name)
        hdf5 = self.get_hdf5_file(hdf5_fname)
        return hdf5.get(hdf5_name, hdf5_ts_name, name, attrs)

    def remove(self, cluster: str, name: str):
        _, _, hdf5_fname, hdf5_name, _ = self.sql.select_serie_by_name(cluster, name)
        self.sql.remove(cluster, name)
        hdf5 = self.get_hdf5_file(hdf5_fname)
        hdf5.remove(hdf5_name)

    def select_series(self, cluster: str, metric: str, start_time: Optional[int], stop_time: Optional[int],
                      **selectors: str) -> Iterator[Serie]:
        it = self.sql.select_series(cluster, metric, start_time, stop_time, **selectors)
        for name, attrs, config_id, _, _, hdf5_fname, hdf5_name, hdf5_ts_name in it:
            hdf5 = self.get_hdf5_file(hdf5_fname)
            yield hdf5.get(hdf5_name, hdf5_ts_name, name, attrs)

    def get_saver(self, cluster: str, config: Optional[str], hdf5_fname: str, compact: bool = False)\
            -> Callable[[Serie], None]:
        hdf5 = self.get_hdf5_file(hdf5_fname)
        def saver_closure(serie: Serie):
            if config:
                try:
                    cid = self.config_ids[config]
                except KeyError:
                    cid = self.config_ids[config] = self.sql.insert_config(config)
            else:
                cid = None

            hdf5_ts_name = self.get_hdf5_ts_name(serie)
            if hdf5_ts_name not in self.existing_ts_hdf5_names:
                hdf5.save_times(hdf5_ts_name, serie.times)
                self.existing_ts_hdf5_names.add(hdf5_ts_name)
            hdf5_name = self.get_hdf5_name(serie)
            if hdf5.save(hdf5_name, serie, compact):
                self.sql.insert_serie(cluster, str(hdf5_fname), hdf5_name, hdf5_ts_name, serie, cid)
        return saver_closure


def open_storage(directory: Path) -> IDataStorage:
    rt = directory.expanduser()
    rt.mkdir(parents=True, exist_ok=True)
    return HDF5AndSqliteDataStorage(rt / 'index.sqlite3', rt)
