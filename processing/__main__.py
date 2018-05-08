import sys
import argparse
from typing import Any, List
from pathlib import Path

import numpy
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
    count = 0
    total = 0
    with make_storage("raw", fname) as src:
        for idx, rec in enumerate(src):
            if rec.metric == "diskio_write_bytes":
                df = make_diff(rec.vals)
                pyplot.plot(numpy.cumsum(df))
                pyplot.plot(rec.vals.clip(0, 10000000))
                pyplot.show()
                return
    print(count, total)


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    process_file(opts.raw_src[0])
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))