import abc
import mmap
import struct
import warnings
from typing import Iterable, Tuple, Dict, Any
from pathlib import Path

import numpy

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import h5py

from .serie import Serie


class INonIndexableStorage(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __iter__(self) -> Iterable[Serie]:
        pass

    @classmethod
    @abc.abstractmethod
    def open(cls, name: str, mode: str = 'r') -> 'INonIndexableStorage':
        pass

    @abc.abstractmethod
    def __enter__(self) -> 'INonIndexableStorage':
        pass

    @abc.abstractmethod
    def __exit__(self, type, value, traceback) -> bool:
        pass


class RAWStorage(INonIndexableStorage):
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
        self.mdf = None
        self.fd = None
        return False

    @staticmethod
    def split_serie_name(name: str) -> Tuple[str, Dict[str, str]]:
        metric, *str_tags = name.split(',')
        return metric, dict(str_tag.split("=", 1) for str_tag in str_tags)

    def unpack_data(self, offset: int, sz: int) -> Tuple[numpy.ndarray, numpy.ndarray]:
        data = numpy.array(
            struct.unpack(">" + "Q" * sz, self.mfd[offset: offset + 8 * sz]),
            dtype=numpy.float64)
        offset += 8 * sz
        ts = numpy.array(
            struct.unpack(">" + "I" * sz, self.mfd[offset: offset + 4 * sz]),
            dtype=numpy.uint32)
        return ts, data

    def __iter__(self) -> Iterable[Serie]:
        offset = 0
        while offset < len(self.mfd):
            # read name
            noffset = self.mfd.find(b'\x00', offset)
            name = self.mfd[offset: noffset].decode("ascii")
            sz, = struct.unpack(">I", self.mfd[noffset + 1: noffset + 5])
            if sz > 1024:
                metric, tags = self.split_serie_name(name)
                if metric == 'diskio_write_bytes' and tags.get("host", "").startswith("ceph"):
                    ts, data = self.unpack_data(noffset + 5, sz)
                    data /= 1E9
                else:
                    data = ts = numpy.array([])
                yield Serie(name, metric, tags, ts, data)
            offset = noffset + 5 + 12 * sz


class IStorage(INonIndexableStorage):
    @abc.abstractmethod
    def save(self, serie: Serie):
        pass

    @abc.abstractmethod
    def get(self, name: str) -> Serie:
        pass


class HDF5Storage(IStorage):
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

    def save(self, serie: Serie):
        dset = self.fd.create_dataset(serie.name, data=numpy.stack([serie.times, serie.vals]))
        for k, v in serie.tags.items():
            assert k not in dset.attrs
            dset.attrs[k] = v

    def get(self, name: str) -> Serie:
        dset = self.fd[name]
        assert len(dset.shape) == 2 and dset.shape[0] == 2
        times = dset[0]
        values = dset[1]
        metric, tags = RAWStorage.split_serie_name(name)
        return Serie(name, metric, tags, times, values)


def make_storage(tp: str, file_name: str, mode: str = "r") -> INonIndexableStorage:
    path = Path(file_name).expanduser()
    if tp == 'raw':
        return RAWStorage.open(path, mode)
    elif tp == 'hdf5':
        return HDF5Storage.open(path, mode)
    raise ValueError(f"Unknown storage type {tp!r}")