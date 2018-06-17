import ctypes
from pathlib import Path
from typing import Callable, cast

import numpy

__all__ = ["allign_diff", "init"]


double_ptr = ctypes.POINTER(ctypes.c_double)
uint32_ptr = ctypes.POINTER(ctypes.c_uint32)


so_allign_func: Callable[[uint32_ptr, ctypes.c_int, double_ptr, ctypes.c_int, uint32_ptr, double_ptr],
                         ctypes.c_int] = None
so_fix_diff_func: Callable[[double_ptr, ctypes.c_int, ctypes.c_double, ctypes.c_int], ctypes.c_int] = None


def allign_func(serie_times: numpy.ndarray, serie_data: numpy.ndarray, expected_tms: numpy.ndarray) -> numpy.ndarray:
    assert serie_times.dtype == numpy.uint32
    assert serie_data.dtype == numpy.double
    assert serie_times.shape == serie_data.shape
    assert serie_times.ndim == 1
    assert expected_tms.dtype == numpy.uint32
    assert expected_tms.ndim == 1
    assert expected_tms.shape[0] >= serie_times.shape[0], f"{expected_tms.shape} >= {serie_times.shape}"
    assert expected_tms[0] <= serie_times[0]
    assert expected_tms[-1] >= serie_times[-1]
    assert expected_tms.strides == (4,)
    assert serie_times.strides == (4,)
    assert serie_data.strides == (8,)

    result = numpy.zeros(expected_tms.shape, dtype=numpy.float64)
    res = so_allign_func(expected_tms.ctypes.data_as(uint32_ptr),
                         expected_tms.shape[0],
                         serie_data.ctypes.data_as(double_ptr),
                         serie_data.shape[0],
                         serie_times.ctypes.data_as(uint32_ptr),
                         result.ctypes.data_as(double_ptr))
    if res != 0:
        raise ValueError(f"so_allign_func failed with code {res}")
    return result


def fix_diff_func(diff_data: numpy.ndarray, top_perc: float, window_sz: int = 16):
    assert diff_data.dtype == numpy.double
    assert diff_data.ndim == 1
    assert diff_data.strides == (8,)

    res = so_fix_diff_func(diff_data.ctypes.data_as(double_ptr),
                           diff_data.shape[0],
                           cast(ctypes.c_double, top_perc),
                           window_sz)
    if res != 0:
        raise ValueError(f"so_diff_func failed with code {res}")


def is_inited() -> bool:
    return so_allign_func is not None


def init(path: Path):
    so_obj = ctypes.cdll.LoadLibrary(path / 'libstages.so')

    global so_allign_func
    so_allign_func = so_obj.allign_times
    so_allign_func.argtypes = [uint32_ptr, ctypes.c_int, double_ptr, ctypes.c_int, uint32_ptr, double_ptr]
    so_allign_func.restype = ctypes.c_int

    global so_fix_diff_func
    so_fix_diff_func = so_obj.fix_diff
    so_fix_diff_func.argtypes = [double_ptr, ctypes.c_int, ctypes.c_double, ctypes.c_int]
    so_fix_diff_func.restype = ctypes.c_int


def allign_diff(serie_times: numpy.ndarray,
                serie_data: numpy.ndarray,
                expected_tms: numpy.ndarray,
                top_perc: int = 95,
                window_sz: int = 16,
                diff: bool = True) -> numpy.ndarray:
    assert is_inited()
    alligned = allign_func(serie_times, serie_data, expected_tms)
    if not diff:
        return alligned

    diff = numpy.diff(alligned)
    perc = numpy.percentile(diff, top_perc)
    fix_diff_func(diff, perc, window_sz)
    return diff
