from pathlib import Path
from abc import ABCMeta, abstractmethod
from typing import Iterator, Optional, Callable, Tuple, Dict


from .serie import Serie


class IDataStorage(metaclass=ABCMeta):
    @abstractmethod
    def select_series(self, cluster: str, metric: str, start_time: Optional[int], stop_time: Optional[int],
                      **selectors: str) -> Iterator[Serie]:
        pass

    @abstractmethod
    def get(self, cluster: str, name: str) -> Serie:
        pass

    @abstractmethod
    def get_saver(self, cluster: str, config: Optional[str], hdf5_fname: str, compact: bool = False) \
            -> Callable[[Serie], None]:
        pass

    @abstractmethod
    def __enter__(self) -> 'IDataStorage':
        return self

    @abstractmethod
    def __exit__(self, type, value, traceback):
        pass

    @abstractmethod
    def remove(self, cluster: str, name: str):
        pass


class IIndex(metaclass=ABCMeta):
    @abstractmethod
    def insert_serie(self,
                     cluster: str,
                     hdf5_file: str,
                     hdf5_name: str,
                     hdf5_ts_name: str,
                     serie: Serie,
                     config_id: int = None):
        pass

    @abstractmethod
    def insert_config(self, cfg: str) -> int:
        pass

    @abstractmethod
    def sync(self):
        pass

    @abstractmethod
    def remove(self, cluster: str, name: str):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def select_series(self,
                      cluster: str,
                      metric: str,
                      start_time: Optional[int],
                      stop_time: Optional[int],
                      **selectors: str) -> Iterator[Tuple[str, Dict[str, str], Optional[int], int, int, str, str, str]]:
        pass

