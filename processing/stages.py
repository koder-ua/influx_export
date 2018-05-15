from functools import wraps
from typing import List, Tuple, Dict, Callable, Iterable, Set

import numpy


ALL_STAGES: Dict[str, Callable[[Iterable[numpy.ndarray]], Iterable[numpy.ndarray]]] = {}


def stage(func):
    assert func.__name__ not in ALL_STAGES
    ALL_STAGES[func.__name__] = func
    return func


def ss_stage(func):
    assert func.__name__ not in ALL_STAGES
    @wraps(func)
    def closure(data: Iterable[numpy.ndarray], *args, **kwargs) -> Iterable[numpy.ndarray]:
        for arr in data:
            yield func(arr, *args, **kwargs)
    ALL_STAGES[func.__name__] = closure(func)
    return func


@ss_stage
def make_diff(arr: numpy.ndarray,
              perc: int = 99,
              min_good_window: int = 16) -> numpy.ndarray:
    diff = numpy.diff(arr)
    top99 = numpy.percentile(diff, perc)

    noisy_parts: List[Tuple[int, int]] = []

    assert min_good_window >= 2

    in_noisy_part = False
    noisy_start_at = None
    clean_start_at = None
    for idx, dval in enumerate(diff):
        if top99 > dval > 0:
            # good point
            if in_noisy_part:
                if clean_start_at is None:
                    clean_start_at = idx
                elif idx - clean_start_at == min_good_window:
                    noisy_parts.append((noisy_start_at, clean_start_at))
                    in_noisy_part = False
                    noisy_start_at = clean_start_at = None
        else:
            if in_noisy_part:
                clean_start_at = None
            else:
                in_noisy_part = True
                noisy_start_at = idx

    for start_idx, end_idx in noisy_parts:
        diff_summ = diff[start_idx: end_idx].sum()
        if diff_summ > 0:
            diff[start_idx: end_idx] = diff_summ / (end_idx - start_idx)
        else:
            diff[start_idx: end_idx] = 0

    return diff


@stage
def skip(data: Iterable[numpy.ndarray], count: int) -> Iterable[numpy.ndarray]:
    for idx, arr in enumerate(data):
        if idx >= count:
            yield arr


@stage
def take(data: Iterable[numpy.ndarray], count: int) -> Iterable[numpy.ndarray]:
    for idx, arr in enumerate(data):
        if idx > count:
            break
        yield arr


def allign_times(ref_map: List[int],
                 data: numpy.ndarray,
                 times: Iterable[int]) -> numpy.ndarray:
    res = numpy.zeros(len(ref_map), dtype=data.dtype)
    it1 = iter(enumerate(ref_map))
    for vl, tm in zip(data, times):
        idx, ref_tm = next(it1)
        while ref_tm < tm:
            res[idx] = 0
            idx, ref_tm = next(it1)
        assert ref_tm == tm, f"Extra time points in input data {ref_tm} {tm}"
        res[idx] = vl

    for idx, _ in it1:
        res[idx] = 0

    return res


def make_times_map(start_time: int, end_time: int, step: int) -> Dict[int, int]:
    return {tm: idx for idx, tm in enumerate(range(start_time, end_time + step, step))}
