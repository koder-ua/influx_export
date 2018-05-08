import sys
import mmap
import numpy
import struct
from typing import Iterable, Tuple, Dict, List
from pathlib import Path

from dataclasses import dataclass
from matplotlib import pyplot


@dataclass
class Record:
    name: str
    metric: str
    tags: Dict[str, str]
    times: numpy.ndarray
    vals: numpy.ndarray

    def __str__(self) -> str:
        return f"R(name='{self.name}', sz={len(self.times)})"


def split_serie_name(name: str) -> Tuple[str, Dict[str, str]]:
    metric, *str_tags = name.split(',')
    return metric, dict(str_tag.split("=", 1) for str_tag in str_tags)


def unpack_data(mfd: mmap.mmap, offset: int, sz: int) -> Tuple[numpy.ndarray, numpy.ndarray]:
    data = numpy.array(
        struct.unpack(">" + "Q" * sz, mfd[offset: offset + 8 * sz]),
        dtype=numpy.float64)
    offset += 8 * sz
    ts = numpy.array(
        struct.unpack(">" + "I" * sz, mfd[offset: offset + 4 * sz]),
        dtype=numpy.uint32)
    return ts, data


def iter_records(mfd: mmap.mmap) -> Iterable[Record]:
    offset = 0
    while offset < len(mfd):
        # read name
        noffset = mfd.find(b'\x00', offset)
        name = mfd[offset: noffset].decode("ascii")
        sz, = struct.unpack(">I", mfd[noffset + 1: noffset + 5])
        if sz > 1024:
            metric, tags = split_serie_name(name)
            if metric == 'diskio_write_bytes' and tags.get("host", "").startswith("ceph"):
                ts, data = unpack_data(mfd, noffset + 5, sz)
                data /= 1E9
            else:
                data = ts = numpy.array([])
            yield Record(name, metric, tags, ts, data)
        offset = noffset + 5 + 12 * sz


def make_diff(arr: numpy.ndarray, max_window: int = 64, min_good_pts: int = 4):
    diff_raw = numpy.diff(arr)
    top99 = numpy.percentile(diff_raw, 99)
    # top99 = 2
    loc_arr = arr.copy()

    normal_state = True
    good_pts_in_row = None
    last_good_idx = None
    last_good_val = None
    first_good_val = None
    first_good_idx = None

    idx = 0
    while idx < len(loc_arr) - 1:
        pval = loc_arr[idx]
        val = loc_arr[idx + 1]

        if normal_state:
            if val < pval or val > pval + top99:
                normal_state = False
                last_good_idx = idx
                last_good_val = pval
                good_pts_in_row = 0
        else:
            vdiff = val - last_good_val
            if 0 < vdiff < top99 * (idx - last_good_idx):
                if good_pts_in_row == 0:
                    first_good_val = val
                    first_good_idx = idx + 1
                # good pts
                good_pts_in_row += 1
                if good_pts_in_row == min_good_pts:
                    step = (first_good_val - last_good_val) / (first_good_idx - last_good_idx)
                    loc_arr[last_good_idx + 1: first_good_idx] = \
                        numpy.arange(1, first_good_idx - last_good_idx) * step + last_good_val
                    normal_state = True
                    first_good_val = first_good_idx = last_good_val = last_good_idx = good_pts_in_row = None
            elif idx - last_good_idx > max_window:
                # assert last_good_val > val
                idx = last_good_idx
                first_good_val = first_good_idx = last_good_val = last_good_idx = good_pts_in_row = None
                normal_state = True
                # this is restart or hight up
                # retart will be cleaned later
            else:
                first_good_val = first_good_idx = None
                good_pts_in_row = 0
        idx += 1

    diff = numpy.diff(loc_arr)
    diff[diff < 0] = diff[diff > 0].mean()
    return diff


def make_diff2(arr: numpy.ndarray, window: int = 64, wperc: float = 0.05) -> numpy.ndarray:
    top99 = numpy.percentile(numpy.diff(arr), 99)
    # top99 = 2
    loc_arr = arr.copy()

    idx = 0
    while idx < len(loc_arr) - 1:
        dval = loc_arr[idx] - loc_arr[idx + 1]

        if top99 > dval > 0 or dval:
            idx += 1
        else:
            for idx2 in range(idx + 2, idx + window):
                total_changes = abs(numpy.diff(loc_arr[idx: idx2])).sum()
                vdiff = loc_arr[idx2] - loc_arr[idx]
                if vdiff > 0 and vdiff < wperc * total_changes:
                    step = vdiff / (idx2 - idx)
                    loc_arr[idx: idx2 - 1] = numpy.arange(1, idx2 - idx) * step + loc_arr[idx]
                    idx = idx2
                    break

    diff = numpy.diff(loc_arr)
    diff[diff < 0] = diff[diff > 0].mean()
    return diff


def make_diff3(arr: numpy.ndarray, perc: int = 99, min_good_window: int = 16) -> numpy.ndarray:
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
        # print(start_idx, end_idx,
        #       int(diff[start_idx: end_idx].sum()),
        #       int(abs(diff[start_idx: end_idx]).sum()),
        #       int(ndiff))
        # new_summ = diff[start_idx: end_idx].sum()
        # print(f"S delta {old_summ - new_summ}")

    return diff


def process_file(fname: str):
    count = 0
    with Path(fname).expanduser().open('r') as fd:
        with mmap.mmap(fd.fileno(), 0, access=mmap.ACCESS_READ) as mfd:
            for rec in iter_records(mfd):
                if rec.metric == "diskio_write_bytes":
                    # if numpy.diff(rec.vals).sum() < 0 and rec.vals.mean() > 1E6:
                    print(count)
                    df = make_diff3(rec.vals)
                    pyplot.plot(numpy.cumsum(df))
                    pyplot.plot(rec.vals.clip(0, 10000000))
                    pyplot.show()

                    if count == 30:
                        exit(0)
                    count += 1


if __name__ == "__main__":
    process_file(sys.argv[1])
    # vls = numpy.array([10, 11, 12, 13, 0, 1, 2, 3, 4, 5, 6])
    # vls = numpy.array([0, 1, 2, 3, 4, 5, 6, 7 - 5, 8 + 5, 9, 10, 11, 12, 13, 14])
    # d1 = make_diff(vls)
    # pyplot.plot(vls)
    # pyplot.plot(numpy.cumsum(d1))
    # pyplot.show()