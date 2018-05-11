import re
import sys
import argparse
from typing import Any, List
from pathlib import Path

import seaborn as sns
from matplotlib import pyplot

from .storage import make_storage
from .stages import make_diff


sns.set_style("darkgrid")


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        default=str(Path(__file__).parent / "config.yaml"))
    parser.add_argument('raw_src', metavar='RAW_SRC', nargs='*')
    return parser.parse_args(args[1:])


def process_file(fname: str):
    ceph_disk_io_aggregate = None
    max_set = set()
    with make_storage("raw", fname) as src:
        for idx, rec in enumerate(src):
            tset = set(rec.times)
            max_set.update(tset)

    with make_storage("raw", fname) as src:
        for idx, rec in enumerate(src):
            if rec.metric == "diskio_write_bytes" and re.match("ceph\\d+", rec.tags['host']):
                tset = set(rec.times)
                extra_points = tset.difference(max_set)
                missing_points = max_set.difference(tset)

                if not extra_points:
                    pass
                elif len(extra_points) > 10:
                    print(f"{len(extra_points)} extra points")
                else:
                    print(f"+{','.join(map(str, sorted(extra_points)))}")

                if not missing_points:
                    pass
                elif len(missing_points) > 10:
                    print(f"{len(missing_points)} missing points")
                else:
                    print(f"-{','.join(map(str, sorted(missing_points)))}")

                continue

                diff = make_diff(rec.vals)
                if ceph_disk_io_aggregate is None:
                    ceph_disk_io_aggregate = diff[:11270]
                elif len(diff) >= 11270:
                    ceph_disk_io_aggregate += diff[:11270]

    #pyplot.plot(ceph_disk_io_aggregate)
    #pyplot.show()


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    process_file(opts.raw_src[0])
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))