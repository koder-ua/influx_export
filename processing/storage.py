import mmap
import struct
import warnings
import datetime
from typing import Iterable, Tuple, Dict, Any, Iterator
from pathlib import Path

import numpy

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import h5py

from .serie import Serie


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
        serie.times, data = self.unpack_data(serie.offset, serie.size)
        serie.vals = data / 1E9

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


class HDF5Storage:
    full_name = "__full_name__"

    def __init__(self, path: Path, fd: h5py.File) -> None:
        self.path = path
        self.fd = fd

    def __enter__(self) -> 'HDF5Storage':
        assert self.fd is not None
        return self

    @classmethod
    def open(cls, name: str, mode: str = 'r') -> 'HDF5Storage':
        path = Path(name).expanduser()
        fd = h5py.File(str(path), mode)
        return cls(path, fd)

    def __exit__(self, x, y, z):
        self.fd.close()
        self.fd = None

    def save(self, name: str, serie: Serie):
        if '/' in name:
            directory, name = name.split('/')
            if directory not in self.fd:
                self.fd.create_group(directory)

        if serie.times is not None:
            data = numpy.stack([serie.times, serie.vals])
        else:
            data = serie.vals

        dset = self.fd.create_dataset(name, data=data)

        for k, v in serie.tags.items():
            assert k not in dset.attrs
            dset.attrs[k] = v
        dset.attrs[self.full_name] = serie.name

    def remove(self, key: str):
        if '/' in key:
            grp_name, name = key.split("/")
            grp = self.fd[grp_name]
            del grp[name]
        else:
            del self.fd[key]

    def __contains__(self, item: str) -> bool:
        return item in self.fd

    def get(self, name: str) -> Serie:
        dset = self.fd[name]
        data = dset.value
        if len(data.shape) == 2:
            assert data.shape[0] == 2
            times = data[0]
            values = data[1]
        else:
            times = None
            values = data

        attrs = dict(dset.attrs.items())
        serie_name = attrs.pop(self.full_name)
        metric, _ = RAWStorage.split_serie_name(serie_name)
        return Serie(serie_name, metric, attrs, times, values)

    def __getitem__(self, name: str) -> Serie:
        return self.get(name)

    def __delitem__(self, name: str):
        return self.remove(name)

    def select_series(self, directory: str, metric: str, **attrs) -> Iterator[Serie]:
        grp = self.fd[directory]
        for name, obj in grp.items():
            if name == metric:
                for k, v in attrs.items():
                    if obj.attrs.get(k, _NoVal) != v:
                        break
                else:
                    yield self.get(directory + '/' + name)
